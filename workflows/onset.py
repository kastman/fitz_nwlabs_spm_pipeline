"""Make SPM *.mat onset files using csv design"""
# import re
import os.path as op
import pandas as pd
from scipy.io import savemat
from numpy import empty
from nipype import Node, SelectFiles, Workflow, IdentityInterface, DataSink
from nipype.interfaces.base import (BaseInterface,
                                    BaseInterfaceInputSpec,
                                    # InputMultiPath,
                                    OutputMultiPath,
                                    TraitedSpec, File, traits,
                                    # isdefined
                                    )
import fitz
# from fitz.tools import ManyOutFiles, SaveParameters, nii_to_png

default_parameters = dict(
    conditions=[],
    condition_col='condition',
    duration_col='duration',
    onset_col='onset',
    run_col='run',
    pmod_cols=[],
    pmod_conditions=[],
    concatenate_runs=False
)


def workflow_manager(project, exp, args, subj_source):
    # ----------------------------------------------------------------------- #
    # Create Onsets
    # ----------------------------------------------------------------------- #

    # Create SPM.mat onsets files from design file.
    onset, onset_input, onset_output = workflow_spec(exp_info=exp)
    onset_base = op.join(project['data_dir'], "{subject_id}/design")
    design_file = exp["design_name"] + ".csv"
    onset_templates = dict(
        design_file=op.join(onset_base, design_file),
    )

    onset_source = Node(SelectFiles(onset_templates), "onsets_source")

    onset_inwrap = fitz.tools.InputWrapper(onset, subj_source,
                                           onset_source, onset_input)
    onset_inwrap.connect_inputs()

    onset_sink = Node(DataSink(base_directory=project['analysis_dir']),
                      "onset_sink")

    onset_outwrap = fitz.tools.OutputWrapper(onset, subj_source,
                                             onset_sink, onset_output)
    onset_outwrap.set_subject_container()
    onset_outwrap.set_mapnode_substitutions(exp["n_runs"])
    onset_outwrap.sink_outputs("models/%s/onset" % exp['model_name'])
    # Insert model name before onset.
    # onset_outwrap.add_regexp_substitutions(("onset", "%s/onset" % exp['model_name']))

    # Set temporary output locations
    onset.base_dir = project['working_dir']

    return onset


def workflow_spec(name="onset", exp_info=None):
    # Default experiment parameters
    if exp_info is None:
        exp_info = fitz.default_experiment_parameters

    # Define constant inputs
    inputs = ["design_file"]

    # Define the workflow inputs
    inputnode = Node(IdentityInterface(inputs), "inputs")

    onsetsetup = Node(OnsetSetup(), "onsetsetup")
    onsetsetup.inputs.exp_info = exp_info
    onsetsetup.inputs.conditions = exp_info['conditions']
    onsetsetup.inputs.condition_col = exp_info['condition_col']
    onsetsetup.inputs.duration_col = exp_info['duration_col']
    onsetsetup.inputs.onset_col = exp_info['onset_col']
    onsetsetup.inputs.run_col = exp_info['run_col']
    onsetsetup.inputs.pmod_cols = exp_info['pmod_cols']
    onsetsetup.inputs.pmod_conditions = exp_info['pmod_conditions']
    onsetsetup.inputs.concatenate_runs = exp_info['concatenate_runs']

    # Define the workflow outputs
    outputnode = Node(IdentityInterface(["design_mats"]),
                      "outputs")

    # Define the workflow and connect the nodes
    onsetFlow = Workflow(name=name)
    onsetFlow.connect([
        (inputnode, onsetsetup,
            [("design_file", "design_file")]),
        (onsetsetup, outputnode,
            [("design_mats", "design_mats")])
    ])

    return onsetFlow, inputnode, outputnode

# =========================================================================== #


class OnsetSetupInput(BaseInterfaceInputSpec):

    exp_info = traits.Dict()
    design_file = File(exists=True)
    conditions = traits.List([], use_default=True)
    run_col = traits.Str('run', use_default=True)
    condition_col = traits.Str('condition', use_default=True)
    onset_col = traits.Str('onset', use_default=True)
    duration_col = traits.Str('duration', use_default=True)
    pmod_cols = traits.List([], use_default=True)
    pmod_conditions = traits.List([], use_default=True)
    concatenate_runs = traits.Bool(False, use_default=True)


class OnsetSetupOutput(TraitedSpec):

    design_mats = OutputMultiPath(File(exists=True))
    # contrast_file = File(exists=True)
    # design_matrix_pkl = File(exists=True)
    # report = OutputMultiPath(File(exists=True))


class OnsetSetup(BaseInterface):

    input_spec = OnsetSetupInput
    output_spec = OnsetSetupOutput

    def _run_interface(self, runtime):
        self._exp_info = self.inputs.exp_info
        self.design_mats = self._mats_from_csv(
            design_file=self.inputs.design_file,
            conditions=self.inputs.conditions,
            condition_col=self.inputs.condition_col,
            onset_col=self.inputs.onset_col,
            duration_col=self.inputs.duration_col,
            pmod_cols=self.inputs.pmod_cols,
            pmod_conditions=self.inputs.pmod_conditions,
            run_col=self.inputs.run_col,
            concatenate_runs=self.inputs.concatenate_runs)

        return runtime

    def _mats_from_csv(self, design_file, conditions, condition_col,
                       onset_col, duration_col, pmod_cols, pmod_conditions,
                       run_col, concatenate_runs):
        runs_df = pd.read_csv(design_file)
        if not len(conditions):
            conditions = runs_df[condition_col].unique()

        if not len(pmod_conditions):
            pmod_conditions = conditions

        # Check to make sure pmods are valid
        self._check_pmod_values(pmod_cols, condition_col, runs_df,
                                concatenate_runs, run_col, pmod_conditions)

        outfiles = []
        for r, run_df in runs_df.groupby(run_col):
            infolist = []
            for cond in conditions:
                onsets = self.onsets_for(cond, run_df, condition_col,
                                         onset_col, duration_col, run_col,
                                         pmod_cols, pmod_conditions)
                if onsets:  # Don't append 0-length onsets
                    infolist.append(onsets)
            outfile = '%s-%s_run%d.mat' % (
                self._exp_info["exp_name"],
                self._exp_info["model_name"],
                int(r))
            scipy_onsets = self._lists_to_scipy(infolist)
            savemat(outfile, scipy_onsets, oned_as='row')
            outfiles.append(op.abspath(outfile))
        return outfiles

    def onsets_for(self, cond, run_df, condition_col='condition',
                   onset_col='onset', duration_col='duration', run_col='run',
                   pmod_cols=[], pmod_conditions=[]):
        """
        Inputs:
          * Condition Label to grab onsets, durations & amplitudes / values.
          * Pandas Dataframe for current run containing onsets values as
            columns.

        Outputs:
          * Returns a dictionary of extracted values for onsets, durations,
            etc.
          * Returns None if there are no onsets.
        """
        condinfo = {}
        cond_df = run_df[run_df[condition_col] == cond]

        if cond_df[onset_col].notnull().any():  # Onsets Present
            if cond_df[duration_col].notnull().any():
                durations = cond_df[duration_col].tolist()
            else:
                durations = [0]

            condinfo = dict(
                name=cond,
                durations=durations,
                onsets=cond_df[onset_col].tolist(),
            )
            # print cond, condinfo

            pmods = []
            # Use lyman-style "value" as a pmod.
            if (('value' in cond_df.columns) and
                    (cond_df['value'].notnull().any()) and
                    (len(cond_df['value'].unique()) > 1)):
                pmods.append(dict(
                    name=self._exp_info.get('pmod_name', 'pmod'),
                    poly=1,
                    param=cond_df['value'].tolist(),
                ))

            # print pmod_conditions
            # print 'check cond ' + cond
            if cond in pmod_conditions:
                for pmod_col in pmod_cols:
                    # print pmod_col, cond
                    pmod_label = self._strip_pmod_label(pmod_col)
                    pmods.append(dict(
                        name=pmod_label,
                        poly=1,
                        param=cond_df[pmod_col].tolist(),
                    ))
            if len(pmods):
                condinfo['pmod'] = pmods
        else:
            print 'Empty onsets for ' + cond
            condinfo = None
        return condinfo

    def _strip_pmod_label(self, pmod_col):
        col_start = pmod_col[5:]
        if col_start[5:] == 'pmod-':
            pmod_label = col_start
        else:
            pmod_label = pmod_col
        return pmod_label

    def _check_pmod_values(self, pmod_cols, condition_col, runs_df,
                           concatenate_runs, run_col, pmod_conditions):
        """Check to make sure pmods are valid
            For each pmod column, make sure there are multiple values per
            condition, or else the columns won't be estimable in the model."""
        for pmod_col in pmod_cols:
            if concatenate_runs:
                cols = condition_col
                label_names = ('condition')
            else:
                cols = [run_col, condition_col]
                label_names = ('run', 'condition')

            for labels, df in runs_df.groupby(cols):
                # Only check for conditions that we want to create pmods for
                if labels[-1] not in pmod_conditions:
                    continue
                pmod_vals = df[pmod_col].unique()
                if len(pmod_vals) == 1:  # Only one value
                    msg = ('Unestimable Pmod %s: only one value ' % pmod_col +
                           '(%s) for %s %s' % (
                                pmod_vals[0], label_names, labels))
                    raise RuntimeError(msg)

    def _lists_to_scipy(self, onsets_list):
        """
        Inputs:
          * List of dicts (one dict for each condition)

            [{'name':'Low','durations':0,'onsets':[1,3,5]},
             {'name':'Hi', 'durations':0, 'onsets':[2,4,6]}]

            - Or with Parametric Modulators -
            [{'name':'Low','durations':0,'onsets':[1,3,5], 'pmod':[
               {'name': 'RT', 'poly':1, 'param':[42,13,666]}]},
             {'name':'High',, 'durations':0, 'onsets':[2,4,6]}]

        Outputs:
          * Dict of scipy arrays for keys names, durations and onsets
            that can be written using scipy.io.savemat
        """

        conditions_n = len(onsets_list)
        names = empty((conditions_n,),     dtype='object')
        durations = empty((conditions_n,), dtype='object')
        onsets = empty((conditions_n,),    dtype='object')

        pmoddt = [('name', 'O'), ('poly', 'O'), ('param', 'O')]
        pmods = empty((conditions_n),      dtype=pmoddt)
        has_pmods = False
        for i, ons in enumerate(onsets_list):
            names[i] = ons['name']
            durations[i] = ons['durations']
            onsets[i] = ons['onsets']
            if 'pmod' not in ons.keys():
                pmods[i]['name'], pmods[i]['poly'], pmods[i]['param'] = [], [], []
            else:
                # 'pmod': [{'name':'rt','poly':1,'param':[1,2,3]}]
                # Multiple pmods per condition are allowed, so pmod
                # is a list of dicts.
                has_pmods = True
                cond_pmod_list = ons['pmod']
                current_condition_n_pmods = len(cond_pmod_list)
                pmod_names = empty((current_condition_n_pmods,), dtype=object)
                pmod_param = empty((current_condition_n_pmods,), dtype=object)
                pmod_poly = empty((current_condition_n_pmods,), dtype=object)

                for pmod_i, val in enumerate(cond_pmod_list):
                    pmod_names[pmod_i] = val['name']
                    pmod_param[pmod_i] = val['param']
                    pmod_poly[pmod_i] = val['poly']

                pmods[i]['name'] = pmod_names
                pmods[i]['poly'] = pmod_poly
                pmods[i]['param'] = pmod_param

        scipy_onsets = dict(
            names=names,
            durations=durations,
            onsets=onsets
        )

        if has_pmods:
            scipy_onsets['pmod'] = pmods

        return scipy_onsets

    def _list_outputs(self):

        outputs = self._outputs().get()
        outputs["design_mats"] = self.design_mats
        # outputs["contrast_file"] = op.abspath("design.con")
        # outputs["design_matrix_pkl"] = op.abspath("design.pkl")
        # outputs["design_matrix_file"] = op.abspath("design.mat")
        return outputs

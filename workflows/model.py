"""Timeseries model using SPM"""
# import re
import os.path as op
from numpy import isnan, any as np_any
from scipy.io import loadmat
# import pandas as pd
# import nibabel as nib
# import matplotlib.pyplot as plt
# from moss import glm
# from moss.mosaic import Mosaic
# import seaborn as sns

from nipype import Node, Workflow, IdentityInterface, SelectFiles, DataSink
# from nipype.interfaces import fsl
from nipype.interfaces import spm
from nipype.pipeline import engine as pe
from nipype.algorithms import modelgen as model
from nipype.interfaces.base import (BaseInterface,
                                    BaseInterfaceInputSpec,
                                    InputMultiPath,
                                    Bunch,
                                    TraitedSpec,
                                    File,
                                    traits,
                                    )
import fitz


default_parameters = dict(
    condition_names=[],
    contrasts=[],
    concatenate_runs=False,
    mask_image=None,
    include_motion=True
)


def workflow_manager(project, exp, args, subj_source):
    # ----------------------------------------------------------------------- #
    # Timeseries Model
    # ----------------------------------------------------------------------- #

    # Create a modelfitting workflow and specific nodes as above
    model, model_input, model_output = workflow_spec(name="model",
                                                     exp_info=exp)

    model_base = op.join(project['analysis_dir'], "{subject_id}")
    model_templates = dict(
        timeseries=op.join(model_base, "preproc", 'sw*.img'),
        realignment_params=op.join(model_base, "preproc", "rp*.txt"),
        onset_files=op.join(model_base, "models",
                            exp["model_name"], "onset",
                            "%s-%s*run*.mat" % (exp["exp_name"],
                                                exp["model_name"])),
        outlier_files=op.join(model_base, "preproc", "art*.txt")
        )

    # if exp["design_name"] is not None:
    #     design_file = exp["design_name"] + "*.mat"
    #     regressor_file = exp["design_name"] + ".csv"
    #     model_templates["design_file"] = op.join(data_dir, "{subject_id}",
    #                                              "design", design_file)
    # if exp["regressor_file"] is not None:
    #     regressor_file = exp["regressor_file"] + ".csv"
    #     model_templates["regressor_file"] = op.join(data_dir, "{subject_id}",
    #                                                 "design", regressor_file)

    model_source = Node(SelectFiles(model_templates), "model_source")

    model_inwrap = fitz.tools.InputWrapper(model, subj_source,
                                           model_source, model_input)
    model_inwrap.connect_inputs()

    model_sink = Node(DataSink(base_directory=project['analysis_dir']),
                      "model_sink")

    model_outwrap = fitz.tools.OutputWrapper(model, subj_source,
                                             model_sink, model_output)
    model_outwrap.set_subject_container()
    model_outwrap.set_mapnode_substitutions(exp["n_runs"])
    model_outwrap.sink_outputs("models/%s/model" % exp['model_name'])

    # Set temporary output locations
    model.base_dir = project['working_dir']

    return model


def workflow_spec(name="model", exp_info=None):
    # Default experiment parameters for generating graph image, testing, etc.
    if exp_info is None:
        exp_info = fitz.default_experiment_parameters

    # Define constant inputs
    inputs = ["realignment_params", "timeseries", "onset_files",
              "outlier_files"]
    #
    # # Possibly add the design and regressor files to the inputs
    # if exp_info["design_name"] is not None:
    #     inputs.append("design_file")
    # if exp_info["regressor_file"] is not None:
    #     inputs.append("regressor_file")

    # Define the workflow inputs
    inputnode = Node(IdentityInterface(inputs), "inputs")

    load_onsets = pe.Node(interface=LoadOnsetsInterface(),
                          name='load_onsets')
    load_onsets.inputs.raise_on_short_conditions = 'raise'

    modelspec = create_modelspec(exp_info)

    level1design = create_level1design(exp_info)

    level1estimate = create_level1estimate(exp_info)

    # Replace the 'T' in the contrast list for nipype (vs. lyman)
    contrastestimate = create_contrastestimate(exp_info)
    contrastestimate.inputs.contrasts = [
        (c[0], 'T', c[1], c[2]) for c in exp_info['contrasts']]

    # Define the workflow and connect the nodes
    model = Workflow(name=name)

    model.connect([
        (inputnode, load_onsets,
            [('onset_files', 'onset_files')]),
        (inputnode, modelspec,
            [('timeseries', 'functional_runs'),
             ('outlier_files', 'outlier_files')]),
        (load_onsets, modelspec,
            [('onsets_infos', 'subject_info')]),
        (modelspec, level1design,
            [('session_info', 'session_info')]),
        (level1design, level1estimate,
            [('spm_mat_file', 'spm_mat_file')]),
        (level1estimate, contrastestimate,
            [('spm_mat_file', 'spm_mat_file'),
             ('beta_images', 'beta_images'),
             ('residual_image', 'residual_image')]),
    ])

    if exp_info['include_motion']:
        model.connect([
            (inputnode, modelspec,
                [('realignment_params', 'realignment_parameters')])
        ])

    # Define the workflow outputs
    outputnode = Node(IdentityInterface(["beta_images",
                                         "mask_image",
                                         "residual_image",
                                         "RPVimage",
                                         "con_images",
                                         "spmT_images",
                                         "spm_mat_file",
                                         #  "censor_rpt",
                                         #  "censor_csv",
                                         #  "censor_json",
                                         ]),
                      "outputs")

    model.connect([
        (level1estimate, outputnode,
            [('beta_images', 'beta_images'),
             ('mask_image', 'mask_image'),
             ('residual_image', 'residual_image'),
             ('RPVimage', 'RPVimage')]),
        (contrastestimate, outputnode,
            [('con_images', 'con_images'),
             ('spmT_images', 'spmT_images'),
             ('spm_mat_file', 'spm_mat_file')]),
        # (threshold_outliers, outputnode,
        #     [('censor_rpt', 'censor_rpt'),
        #      ('censor_csv', 'censor_csv'),
        #      ('censor_json', 'censor_json')]),
    ])

    return model, inputnode, outputnode


# =========================================================================== #
# Northwest Labs Preprocessing Shared Workflows                               #
# ekk 12Nov12                                                                 #
# =========================================================================== #


def create_modelspec(exp_info, name='modelspec'):
    """Generate SPM-specific design information using
    :class:`nipype.interfaces.spm.SpecifyModel`.
    """
    modelspec = pe.Node(interface=model.SpecifySPMModel(),
                        name="modelspec", run_without_submitting=True)
    modelspec.inputs.concatenate_runs = exp_info['concatenate_runs']
    modelspec.inputs.input_units = exp_info['input_units']
    modelspec.inputs.output_units = 'secs'
    modelspec.inputs.high_pass_filter_cutoff = exp_info['hpcutoff']
    modelspec.inputs.time_repetition = exp_info['TR']

    return modelspec


def create_level1design(exp_info, name='level1design'):
    """Generate a first level SPM.mat file for analysis
    :class:`nipype.interfaces.spm.Level1Design`.
    """
    level1design = pe.Node(interface=spm.Level1Design(), name=name)
    level1design.inputs.timing_units = exp_info['output_units']
    level1design.inputs.bases = exp_info['bases']
    level1design.inputs.interscan_interval = exp_info['TR']
    if exp_info['mask_image']:
        level1design.inputs.mask_image = exp_info['mask_image']
    return level1design


def create_level1estimate(exp_info, name='level1estimate'):
    """Use :class:`nipype.interfaces.spm.EstimateModel` to determine the
    parameters of the model.
    """
    level1estimate = pe.Node(interface=spm.EstimateModel(), name=name)
    level1estimate.inputs.estimation_method = {
        exp_info['estimation_method']: 1}
    return level1estimate


def create_contrastestimate(exp_info, name='contrastestimate'):
    """Use :class:`nipype.interfaces.spm.EstimateContrast` to estimate the
    first level contrasts specified in the task_list dictionary above.
    """
    contrastestimate = pe.Node(interface=spm.EstimateContrast(), name=name)
    return contrastestimate


class LoadOnsetsInterfaceInputSpec(BaseInterfaceInputSpec):
    """####### Begin LoadOnsetsInterface ########"""
    onset_files = InputMultiPath(
        traits.Either(
            traits.List(File(exists=True)),
            File(exists=True)),
        desc='Computed MATLAB .mat SPM onsets files', mandatory=True)
    raise_on_short_conditions = traits.Enum(
        'raise', 'remove', 'ignore',
        desc='Raise an error or remove empty conditions?', usedefault=True)
    conditions_list = traits.List(
        desc='Optional list of conditions to include. ' +
        'Loads all conditions in file if not present')
    raise_on_nan_values = traits.Enum(
        'raise', 'ignore',
        desc='Raise an error or remove values that are nan?', usedefault=True)


class LoadOnsetsInterfaceOutputSpec(TraitedSpec):
    onsets_infos = traits.List(desc="Loaded Nipype Onsets Bunches")


class LoadOnsetsInterface(BaseInterface):
    '''The LoadOnsets Interface just reads a correctly formatted
    MATLAB .mat SPM onsets file from disk and returns a Nipype Onsets Bunch.

    Loosely based on the standard subjectinfo method from several Nipype
    examples.'''
    input_spec = LoadOnsetsInterfaceInputSpec
    output_spec = LoadOnsetsInterfaceOutputSpec

    def _run_interface(self, runtime):
        self.onsets_infos = []
        conditions_list = self.inputs.conditions_list

        for srcmat in self.inputs.onset_files:
            if not op.exists(srcmat):
                raise IOError("Can't find input onsets file %s" % srcmat)
            mat = loadmat(srcmat)
            if 'names' not in mat.keys():
                raise KeyError("Onsets file %s doesn't appear to be a valid " +
                               "SPM multiple regressors file; found keys %s " +
                               "instead of ['names', 'onsets', 'durations']. "
                               % (srcmat, mat.keys()))

            # SPM Multiple Regressors Files should be all rows. The cell
            # structure creates double layers, so to get each item when loaded
            # address one level in, and then to get each item address one level
            # in again.
            nConditions = len(mat['names'][0])
            names, durations, onsets, pmods = [], [], [], []

            for i in range(nConditions):
                # Go through each condition and cast it into Stdlibrary strings
                # and lists for Nipype
                name = str(mat['names'][0][i][0])
                # print mat['onsets'][0]
                # print mat['onsets'][0][i][0]
                if conditions_list and (name not in conditions_list):
                    print 'Skipping %s' % name
                    continue
                else:
                    print 'Found %s' % name
                duration = list(mat['durations'][0][i].flatten().tolist())

                if len(mat['onsets'][0][i]):
                    onset = list(mat['onsets'][0][i][0])
                else:
                    onset = []

                names.append(name)
                durations.append(duration)
                onsets.append(onset)

                pmod = self._get_pmods(mat, i)
                if pmod:
                    pmods.append(pmod)

            if conditions_list:
                for cond in conditions_list:
                    if cond not in names:
                        raise RuntimeError("Warning, condition %s not found in"
                                           " %s, got %s"
                                           % (cond, srcmat, names))

            for index, onset in enumerate(onsets):
                # print index,onset,len(onset)
                if len(onset) == 0:
                    if self.inputs.raise_on_short_conditions == 'raise':
                        raise AssertionError("Condition %s is too short to be "
                                             "a valid condition: %s (%s)"
                                             % (names[index], onset, srcmat))
                    elif self.inputs.raise_on_short_conditions == 'remove':
                        action = "removing"
                        names.pop(index)
                        durations.pop(index)
                        onsets.pop(index)
                    else:
                        action = "ignoring"
                    print ("==> Condition %s was short for file %s "
                           "(length %d), %s..."
                           % (names[index], srcmat, len(onset), action))
                if isnan(sum(onset)):
                    if self.inputs.raise_on_nan_values == 'raise':
                        raise AssertionError("Condition %s contains NaNs and "
                                             "raise_on_nan_values is true."
                                             " %s (%s)"
                                             % (names[index], onset, srcmat))
                    else:
                        print ("==> Condition %s for file %s contains NaN"
                               "values, but continuing anyway..."
                               % (names[index], srcmat))

            onset_info = Bunch(dict(
                conditions=names,
                onsets=onsets,
                durations=durations
            ))

            if len(pmods):
                onset_info.pmod = pmods

            self.onsets_infos.append(onset_info)

        return runtime

    def _get_pmods(self, mat, cond_pos, pmod_key='pmod'):
        """Return list of pmod bunches extracted from pmod formatted struct/
           cell arrays, or None if no pmods."""
        # If there is a pmod for this condition
        if (pmod_key in mat.keys() and
                cond_pos < mat[pmod_key]['name'][0].shape[0]):
            n_pmods_per_condition = mat[pmod_key]['name'][0][cond_pos].shape[1]

            pmod_names = []
            pmod_params = []
            pmod_polys = []
            for p in range(n_pmods_per_condition):
                p_name = mat[pmod_key]['name'][0][cond_pos][0][p][0]
                p_param = mat[pmod_key]['param'][0][cond_pos][0][p][0]
                p_poly = mat[pmod_key]['poly'][0][cond_pos][0][p][0]

                if np_any(p_param):
                    pmod_names.append(p_name),
                    pmod_params.append(p_param.tolist()),
                    pmod_polys.append(p_poly.tolist())
                else:
                    print "WARN - Empty pmod for %s" % p_name
            if len(pmod_names):
                pmod = Bunch(
                    name=pmod_names,
                    param=pmod_params,
                    poly=pmod_polys
                )
            else:
                pmod = None

            return pmod

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs["onsets_infos"] = self.onsets_infos
        return outputs

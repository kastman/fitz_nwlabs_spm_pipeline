"""Preprocessing workflow definition."""
import os
import os.path as op
# import numpy as np
# import pandas as pd
# import nibabel as nib
# import matplotlib.pyplot as plt


from nipype.interfaces import spm
from nipype.pipeline import engine as pe
from nipype import (Node, MapNode, SelectFiles, Workflow,
                    IdentityInterface, DataSink)
from nipype.interfaces.base import (BaseInterface,
                                    BaseInterfaceInputSpec,
                                    File,
                                    traits,
#                                     InputMultiPath, OutputMultiPath,
#                                     TraitedSpec
)

import fitz
from fitz.tools import (SingleInFile, SingleOutFile, ManyOutFiles,
                        list_out_file)
from nibabel import load as nib_load, Nifti1Pair


def default_parameters():
    return dict(
        n_runs=0,
        TR=2,
        frames_to_toss=0,
        temporal_interp=False,
        interleaved=True,
        slice_order="up",
        intensity_threshold=4.5,
        motion_threshold=1,
        spike_threshold=None,
        smooth_fwhm=6,
        hpf_cutoff=128,
    )


def workflow(project, exp, args, subj_source):
    """
    # ---------------------- #
    # Preprocessing Workflow #
    # ---------------------- #

    Wrapped with Data Source and Sink
    """

    # Create workflow in function defined elsewhere in this package
    preproc, preproc_input, preproc_output = create_preprocessing_workflow(
                                                exp_info=exp)

    # Collect raw nifti data
    preproc_templates = dict(timeseries=exp["func_template"],
                             anat=exp["anat_template"])

    preproc_source = Node(SelectFiles(preproc_templates,
                                      base_directory=project["data_dir"]),
                          "preproc_source")

    # Convenience class to handle some sterotyped connections
    # between run-specific nodes (defined here) and the inputs
    # to the prepackaged workflow returned above
    preproc_inwrap = fitz.tools.InputWrapper(preproc, subj_source,
                                             preproc_source, preproc_input)
    preproc_inwrap.connect_inputs()

    # Store workflow outputs to persistant location
    preproc_sink = Node(DataSink(base_directory=project['analysis_dir']),
                        "preproc_sink")

    # Similar to above, class to handle sterotyped output connections
    preproc_outwrap = fitz.tools.OutputWrapper(preproc, subj_source,
                                               preproc_sink, preproc_output)
    preproc_outwrap.set_subject_container()
    preproc_outwrap.set_mapnode_substitutions(exp["n_runs"])
    preproc_outwrap.sink_outputs("preproc")

    # Set the base for the possibly temporary working directory
    preproc.base_dir = project['working_dir']

    return preproc


def create_preprocessing_workflow(name="preproc", exp_info=None):
    """Return a Nipype workflow for fMRI preprocessing.

    Parameters
    ----------
    name : string
        workflow object name
    exp_info : dict
        dictionary with experimental information

    Create an SPM preprocessing workflow that runs basic volumetric
    preprocessing. It first motion-corrects and coregisters fMRI runs with a
    T1, and then uses DARTEL or Unified Segmentation (US) to normalize and
    segment a high-res T1. It then applies the warp fields to normalize both
    the functionals and structurals, and smooths the structurals.

    Options are set by manually specifying inputs to internal nodes.

    Example
    -------

    >>> preproc = cbscentral_workflows.create_preproc_workflow()
    >>>

    Inputs::

         inputspec.func_images     : List of input functional 4d NIfTIs
         inputspec.struct_images   : List of Structural NIfTI Images
                                     (can be only 1 long)

    Outputs:

         outputnode.func_images    : List of output functional 4d NIfTIs
         outputnode.struct_images  : List of output Structural NIfTI Images
    """
    preproc = Workflow(name)

    if exp_info is None:
        exp_info = fitz.default_experiment_parameters()

    # Define the inputs for the preprocessing workflow
    in_fields = ["timeseries", "subject_id", "anat"]

    inputnode = Node(IdentityInterface(in_fields), "inputs")

    spminfo = spm.Info.version()

    # Remove equilibrium frames and convert to float
    prep_timeseries = MapNode(PrepTimeseries(), "in_file", "prep_timeseries")
    prep_timeseries.inputs.frames_to_toss = exp_info["frames_to_toss"]

    # Create nodes first, then connect them.
    # For branching workflows this means two separate if's, but it's better
    # to keep creation and connection together than minimize branches.

    # Motion and slice time correct
    if exp_info["temporal_interp"]:
        slicetiming = create_slicetiming(
            TR=exp_info["TR"],
            slice_order_label=exp_info["slice_order"],
            interleaved=exp_info["interleaved"],
            num_slices=exp_info['num_slices'])

    realign = create_realignment()

    # Estimate a registration from funtional to anatomical space
    coregister = create_coregister()

    segment = create_segment(spminfo)

    """Apply Deformation to Functionals"""
    apply_deform_func = create_apply_deformation(spminfo, 'apply_deform_func')

    """Apply Deformation to Structurals"""
    apply_deform_struct = create_apply_deformation(spminfo, 'apply_deform_struct')

    smooth = create_smooth()

    # # Save the experiment info for this run
    # saveparams = MapNode(SaveParameters(exp_info=exp_info),
    #                      "in_file", "saveparams")

    # Connection Helpers
    def first_item(items):
        if isinstance(items, list):
            return items[0]
        else:
            return items

    preproc.connect([
        (inputnode, prep_timeseries,
            [('timeseries', 'in_file')])
    ])

    if exp_info['temporal_interp']:
        preproc.connect([
            (prep_timeseries, slicetiming,
                [('out_file', 'in_files')]),
            (slicetiming, realign,
                [('timecorrected_files', 'in_files')]),
        ])
    else:
        preproc.connect([
            (prep_timeseries, realign,
                [('out_file', 'in_files')])
        ])

    preproc.connect([
        (inputnode, coregister,
            [(('anat', first_item), 'target')]),
        (inputnode, segment,
            [('anat', 'channel_files')]),
        (inputnode, apply_deform_struct,
            [('anat', 'in_files')]),
        (realign, coregister,
            [('mean_image', 'source'),
             ('modified_in_files', 'apply_to_files')]),
        (segment, apply_deform_struct,
            [(('forward_deformation_field', first_item), 'deformation_field')]),
        (segment, apply_deform_func,
            [(('forward_deformation_field', first_item), 'deformation_field')]),
        (coregister, apply_deform_func,
            [('coregistered_files', 'in_files')]),
        (apply_deform_func, smooth,
            [('out_files', 'in_files')]),
    ])

    # Define the outputs of the top-level workflow
    output_fields = ["smoothed_timeseries",
                     "realignment_parameters",
                     "normalized_anat",
                     "transformation_mat",
                     "forward_deformation_field",
                     "native_class_images",
                     "json_file"]

    outputnode = Node(IdentityInterface(output_fields), "outputs")

    preproc.connect([
        (realign, outputnode,
            [("realignment_parameters", "realignment_parameters")]),
        (smooth, outputnode,
            [("smoothed_files", "smoothed_timeseries")]),
        (apply_deform_struct, outputnode,
            [("out_files", "normalized_anat")]),
        (segment, outputnode,
            [("transformation_mat", "transformation_mat"),
             ('forward_deformation_field', 'forward_deformation_field'),
             ('native_class_images', 'native_class_images')]),
        ])

    return preproc, inputnode, outputnode


# =========================================================================== #
# Northwest Labs Preprocessing Shared Workflows                               #
# ekk 12Nov12                                                                 #
# =========================================================================== #

class PrepTimeseriesInput(BaseInterfaceInputSpec):

    in_file = File(exists=True)
    frames_to_toss = traits.Int()


class PrepTimeseries(BaseInterface):

    input_spec = PrepTimeseriesInput
    output_spec = SingleOutFile

    def _run_interface(self, runtime):

        # Load the input timeseries
        img = nib_load(self.inputs.in_file)
        data = img.get_data()
        aff = img.get_affine()
        hdr = img.get_header()

        # Trim off the equilibrium TRs
        data = self.trim_timeseries(data)

        # Save the output timeseries as NIFTI1_PAIR (for spm .mat registration)
        new_img = Nifti1Pair(data, aff, hdr)
        self.fname = op.splitext(op.basename(self.inputs.in_file))[0] + ".img"
        new_img.to_filename(self.fname)

        return runtime

    def trim_timeseries(self, data):
        """Remove frames from beginning of timeseries."""
        return data[..., self.inputs.frames_to_toss:]

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs["out_file"] = op.abspath(self.fname)
        return outputs

def create_slicetiming(TR, slice_order_label, interleaved, num_slices):
    slicetiming = pe.Node(interface=spm.SliceTiming(), name="slicetiming")
    slicetiming.inputs.ref_slice = 1

    if interleaved:
        if slice_order_label == 'down':
            # 'interleaved - top -> down':
            slice_order = range(num_slices, 0, -2) + range(num_slices - 1, 1, -2)
        elif slice_order_label == 'up':
            # 'interleaved - bottom -> up':
            slice_order = range(1, num_slices + 1, 2) + range(2, num_slices + 1, 2)
    else:
        if slice_order_label == 'down':
            # 'sequential - ascending (top slice last)'
            slice_order = range(num_slices, 0, -1)
        elif slice_order_label == 'up':
            # 'sequential - descending (top slice first)':
            slice_order = range(1, num_slices + 1, 1)

    slicetiming.inputs.time_repetition = TR
    slicetiming.inputs.time_acquisition = TR - TR/num_slices
    slicetiming.inputs.num_slices = num_slices
    slicetiming.inputs.slice_order = slice_order

    return slicetiming


def create_realignment():
    realign = pe.Node(interface=spm.Realign(), name="realign")
    realign.inputs.register_to_mean = True
    realign.inputs.jobtype = 'estwrite'
    realign.inputs.write_which = [0, 1]

    return realign

    # """Use :class:`prison_pilot_interfaces.qaMotionSummary` to generate
    # motion summary graphs and text files for motion exclusion."""
    # qaMotionSummary = pe.Node(interface=prison_pilot_interfaces.qaMotionSummary(), name='qaMotionSummary', run_without_submitting=True)


def create_coregister(name='coregister'):
    """Use :class:`nipype.interfaces.spm.Coregister` to perform a rigid
    body registration of the functional data to the structural data.
    """

    coregister = pe.Node(interface=spm.Coregister(), name=name)
    coregister.inputs.jobtype = 'estimate'

    return coregister


def create_segment(spminfo, name='seg'):
    """Create non-linear transformation to MNI space using
    :class:`nipype.interfaces.spm.NewSegment`"""
    template = os.path.join(spminfo['path'], 'toolbox', 'Seg', 'TPM.nii')
    seg = pe.Node(interface=spm.NewSegment(), name=name)
    tissue1 = ((template, 1), 2, (True, True), (True, True))  # Gray
    tissue2 = ((template, 2), 2, (True, True), (True, True))  # White
    tissue3 = ((template, 3), 2, (False, False), (False, False))  # CSF
    tissue4 = ((template, 4), 2, (False, False), (False, False))
    tissue5 = ((template, 5), 2, (False, False), (False, False))
    tissue6 = ((template, 6), 2, (False, False), (False, False))
    seg.inputs.tissues = [tissue1, tissue2, tissue3, tissue4, tissue5, tissue6]
    seg.inputs.write_deformation_fields = [True, True]
    # deformation fields to write: [Inverse, Forward]

    return seg


def create_apply_deformation(spminfo, name='apply_deform'):
    """Apply Deformation Fields to native-space images"""
    apply_deform = pe.Node(interface=spm.preprocess.ApplyDeformations(),
                           name=name)
    template = os.path.join(spminfo['path'], 'templates', 'EPI.nii')
    apply_deform.inputs.reference_volume = template

    return apply_deform


def create_smooth(fwhm=6, name='smooth'):
    """Smooth the functional data using
    :class:`nipype.interfaces.spm.Smooth`.
    """

    smooth = pe.Node(interface=spm.Smooth(), name=name)
    smooth.inputs.fwhm = fwhm

    return smooth

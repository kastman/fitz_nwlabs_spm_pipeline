"""Preprocessing workflow definition."""
# import os
# import os.path as op
from nipype.interfaces import (dcmstack,
                               utility as util,
                               io as nio)
from nipype.pipeline import engine as pe
from nipype import (Node, Workflow,  # MapNode, SelectFiles,
                    IdentityInterface, DataSink)
from nipype.interfaces.base import (BaseInterface,
                                    BaseInterfaceInputSpec,
                                    # File,
                                    traits,
                                    # InputMultiPath, OutputMultiPath,
                                    TraitedSpec
                                    )
import xml.etree.ElementTree
import os.path as op

import fitz
# from fitz.tools import (SingleOutFile, #  ManyOutFiles,  SingleInFile,
#                         list_out_file)
# from nibabel import load as nib_load, Nifti1Pair


def default_parameters():
    return dict(
        struct_offset=0,
        func_offset=0,
        xnat_project=None,
        struct_pattern=None,
        func_pattern=None,
        server=None
    )


def workflow(project, exp, args, subj_source):
    """
    # ------------------------------------------- #
    # Xnat Download and Nifti Conversion Workflow #
    # ------------------------------------------- #

    Wrapped with Data Source and Sink
    """

    # Create workflow in function defined elsewhere in this package
    (xnatconvert, xnatconvert_input,
        xnatconvert_output) = create_xnatconvert_subject_workflow(exp_info=exp)

    xnatconvert.connect([
        (subj_source, xnatconvert_input, [('subject_id', 'subject_id')])
    ])

    # # Collect raw nifti data
    # xnatconvert_templates = dict()
    # #
    # # Source is a source for both datasource and infosource
    # # (so files and iterables)
    # xnatconvert_source = Node(SelectFiles(xnatconvert_templates,
    #                                       base_directory=project["data_dir"]),
    #                           "xnatconvert_source")
    # #
    # # Convenience class to handle some sterotyped connections
    # # between run-specific nodes (defined here) and the inputs
    # # to the prepackaged workflow returned above
    # xnatconvert_inwrap = fitz.tools.InputWrapper(xnatconvert, subj_source,
    #     xnatconvert_source, xnatconvert_input)
    # xnatconvert_inwrap.connect_inputs()

    # Store workflow outputs to persistant location
    xnatconvert_sink = Node(DataSink(base_directory=project['data_dir']),
                            "xnatconvert_sink")

    # Similar to above, class to handle sterotyped output connections
    xnatconvert_outwrap = fitz.tools.OutputWrapper(xnatconvert, subj_source,
        xnatconvert_sink, xnatconvert_output)
    xnatconvert_outwrap.set_subject_container()
    xnatconvert_outwrap.set_mapnode_substitutions(exp["n_runs"],
        template_pattern='', template_args='')  # Replace "_convert0" with ""
    xnatconvert_outwrap.sink_outputs("images")

    # Set the base for the possibly temporary working directory
    xnatconvert.base_dir = project['working_dir']

    return xnatconvert


def create_xnatconvert_subject_workflow(name="xnatconvert", exp_info=None):
    """Return a Nipype workflow for XNAT Download and Conversion.

    Parameters
    ----------
    name : string
        workflow object name
    exp_info : dict
        dictionary with experimental information

    Download specified dicoms for structural and funcitonal imageas and convert
    them to nifti files.

    Outputs:

         outputnode.func_images    : List of output functional 4d NIfTIs
         outputnode.struct_images  : List of output Structural NIfTI Images
    """
    if exp_info is None:
        exp_info = fitz.default_experiment_parameters()

    # Define the inputs for the preprocessing workflow
    in_fields = ['subject_id',
                 'session_label',
                 'xnat_config',
                 'xnat_project',
                 'subject_id',
                 'exam_id',
                 'struct_pattern',
                 'struct_offset',
                 'func_pattern',
                 'func_offset',
                 'cache_dir',
                 'server',
                 'server_alias'
                 ]

    inputnode = Node(IdentityInterface(in_fields), "inputs")

    # inputnode.inputs.xnat_config    = set_xnat_config()
    # inputnode.inputs.server = exp_info['server']
    # inputnode.inputs.cache_dir      = config.get('dicom_cache_dir', os.path.abspath('./DICOMS'))
    # inputnode.inputs.struct_offset  = config.get('struct_offset',0)
    inputnode.inputs.struct_offset = exp_info['struct_offset']
    inputnode.inputs.func_offset = exp_info['func_offset']
    # inputnode.inputs.func_offset    = config.get('func_offset',0)
    # inputnode.inputs.exam_id        = config['examid']
    # inputnode.inputs.session_label  = config['subid']
    # inputnode.inputs.subject_id     = config['subid']
    inputnode.inputs.xnat_project = exp_info['xnat_project']
    inputnode.inputs.struct_pattern = exp_info['struct_pattern']
    inputnode.inputs.func_pattern = exp_info['func_pattern']
    inputnode.inputs.server_alias = exp_info['server_alias']

    struct_flow = create_xnatconvert_flow(name='xnatconvert_struct')
    func_flow = create_xnatconvert_flow(name='xnatconvert_func')

    xnatconvert_subject = Workflow(name)
    xnatconvert_subject.connect([
        (inputnode, struct_flow,
            [('struct_pattern', 'xnatSearch.desc_pattern'),
             ('struct_offset', 'xnatSearch.series_offset'),
             ('subject_id', 'xnatSearch.session_label'),
             ('xnat_project', 'xnatSource.xnat_project'),
            #  ('subject_id', 'xnatSource.subject_id'),
             ('subject_id', 'xnatSource.exam_id'),
             ('server_alias', 'xnatServerConfig.alias'),
             ]),
        (inputnode, func_flow,
            [('func_pattern', 'xnatSearch.desc_pattern'),
             ('func_offset', 'xnatSearch.series_offset'),
             ('subject_id', 'xnatSearch.session_label'),
             ('xnat_project', 'xnatSource.xnat_project'),
            #  ('subject_id', 'xnatSource.subject_id'),
             ('subject_id', 'xnatSource.exam_id'),
             ('server_alias', 'xnatServerConfig.alias')
             ]),
    ])

        # (inputnode, xnatSource,
            # [('xnat_project', 'xnatSource.xnat_project'),
            #  ('subject_id', 'xnatSource.subject_id'),
            #  ('exam_id', 'exam_id'),
            #  (('xnat_config', get_val_for, 'user' ), 'user'),
            #  (('xnat_config', get_val_for, 'pwd' ), 'pwd'),
            #  (('xnat_config', get_val_for, 'server' ), 'server')]),


    # Define the outputs of the top-level workflow
    output_fields = ["struct_images", "func_images"]
    outputnode = Node(IdentityInterface(output_fields), "outputs")

    xnatconvert_subject.connect([
        (struct_flow, outputnode,
            [("convert.out_file", "struct_images")]),
        (func_flow, outputnode,
            [("convert.out_file", "func_images")]),
    ])

    return xnatconvert_subject, inputnode, outputnode


def create_xnatconvert_flow(name):
    xnatIdent = create_serverconfig(name='xnatServerConfig')
    xnatSearch = create_search(name='xnatSearch')
    xnatSource = create_source(name='xnatSource')
    convert = create_convert(name='convert')

    workflow = pe.Workflow(name=name)
    workflow.connect([
        (xnatIdent, xnatSearch,
            [('config', 'xnat_config')]),
        (xnatIdent, xnatSource,
            [('user', 'user'),
             ('pwd', 'pwd'),
             ('server', 'server')]),
        (xnatSearch, xnatSource,
            [('scans', 'scan'),
             ('subject', 'subject_id')]),
        (xnatSource, convert,
            [(('dicoms', select_dicoms), 'dicom_files')]),
    ])

    return workflow


def select_dicoms(lists):
    if not isinstance(lists[0], list):
        lists = [lists]
    # return [[item for item in l if item.endswith('dcm') or item.endswith('IMA')] for l in lists]
    return lists


def clean_list(images_list):
    flatten = lambda nested_list: [item for sublist in nested_list for item in sublist]
    if isinstance(images_list[0], list):
        dicoms = flatten(images_list)
    else:
        dicoms = images_list
    return [item for item in dicoms if item.endswith('dcm') or item.endswith('IMA')]


def get_val_for(d, key):
    return d[key]

    # # Define Nodes for Manual QC Checks
    # # =================================
    # if checkManualQc:
    #     def get_qcQuery(exam_id,scan_ids=[]):
    #         # scan_id is not a scan's uid, but more like series number.
    #         # However, xnat calls it scan_id, so we're staying with xnat's
    #         # query nomenclature for consistency.
    #         query = [('neuroinfo:manualboldqc/EXPT_ID','LIKE','%s%%' % exam_id)]
    #         scan_params = [('neuroinfo:manualboldqc/SCAN_ID','LIKE',str(id)) for id in scan_ids]
    #         scan_params.append('OR')
    #         query.append(scan_params)
    #         query.append('AND')
    #
    #         # query = [('neuroinfo:manualboldqc/EXPT_ID','LIKE','%s%%' % exam_id),
    #         #            [  ('neuroinfo:manualboldqc/SCAN_ID','LIKE','9'),
    #         #               ('neuroinfo:manualboldqc/SCAN_ID','LIKE','12'),
    #         #               'OR'],
    #         #            'AND']
    #
    #         return query
    #
    #     build_qcQuery = pe.Node(interface=util.Function(input_names=['exam_id','scan_ids'],
    #                                 output_names=['query'],
    #                                 function=get_qcQuery),
    #                             name='build_qcQuery', run_without_submitting=True)
    #
    #     qcSearch = pe.Node(interface=cbscentral_interfaces.XnatQuery(), name='qcSearch')
    #     qcSearch.inputs.query_datatype = 'neuroinfo:manualboldqc'
    #
    #     def choose_scans(scan_ids, scan_qcs, remove=['FAIL']):
    #         """ Select Scans and Run Numbers/Orders of scans with passing Manual QC Checks
    #         """
    #         passing_scans = []
    #         passing_orders = []
    #         use_all = False if scan_qcs else True  # Use all scans if NO assessments were found
    #         if use_all:
    #             passing_scans = scan_ids
    #             passing_orders = range(len(scan_ids))
    #
    #
    #         for order, scan_id in enumerate(scan_ids):
    #             qc = scan_qcs.where(scan_id=str(scan_id))
    #             if qc.has_header('overall'):
    #                 if qc['overall'] not in remove:
    #                     passing_scans.append(scan_id)
    #                     passing_orders.append(order)
    #                     scan_msg = "%sed Manual QC - %s" % (qc['overall'].capitalize(), qc['comments'])
    #                 else:
    #                     scan_msg = "Failed Manual QC - %s\n%s" % (qc['comments'], qc.dumps_json())
    #             else:
    #                 scan_msg = "No Manual Assessment found."
    #             print "Scan Series %s: %s" % (scan_id, scan_msg)
    #
    #         if not passing_scans:
    #             raise RuntimeError("No scans passed Manual QC.")
    #         elif len(passing_scans) == 1:
    #             raise RuntimeError("Only 1 passing scan found. Aborting.")
    #
    #         return passing_scans, passing_orders
    #
    #     qcSelect = pe.Node(interface=util.Function(input_names=['scan_ids','scan_qcs'],
    #                                                output_names=['passing_scans','passing_orders'],
    #                                                function=choose_scans), name='qcSelect', run_without_submitting=True)
    #     outputnode_fields.extend(['scan_nums','runs'])
    #
    # outputnode = pe.Node(interface=util.IdentityInterface(fields=outputnode_fields), name='outputnode', run_without_submitting=True)

def exam_cache_dir(cache_dir,exam_id):
    import os
    return os.path.join(cache_dir,exam_id)

    #
    # if checkManualQc:
    #     workflow.connect([
    #         (inputnode, qcSearch, [(('xnat_config', get_val_for, 'user' ), 'user'),
    #                                (('xnat_config', get_val_for, 'pwd' ), 'pwd'),
    #                                (('xnat_config', get_val_for, 'server' ), 'server')]),
    #         (inputnode, build_qcQuery,      [('exam_id', 'exam_id')]),
    #         (xnatFuncSearch, build_qcQuery, [('scan', 'scan_ids')]),
    #         (build_qcQuery, qcSearch,       [('query', 'query')]),
    #         (xnatFuncSearch, qcSelect,      [('scan', 'scan_ids')]),
    #         (qcSearch, qcSelect,            [('result', 'scan_qcs')]),
    #         (qcSelect, outputnode,          [('passing_scans', 'scan_nums'),
    #                                          ('passing_orders', 'runs')]),
    #         (qcSelect, xnatFuncSource,      [('passing_scans', 'scan')])
    #     ])
    # else:
    #     workflow.connect([
    #         (xnatFuncSearch, xnatFuncSource, [('scan', 'scan')]),
    #     ])

def create_serverconfig(name):
    config = pe.Node(interface=XnatServerConfigInterface(), name=name)
    return config

def create_search(name):
    # search = pe.Node(interface=util.Function(input_names=['session_label', 'desc_pattern', 'xnat_config', 'series_offset'],
    #                                          output_names='scan', function=xnat_search),
    #                  name=name)
    search = pe.Node(interface=XnatSearchInterface(), name=name)
    return search


def create_source(name):
    source = pe.Node(interface=nio.XNATSource(infields=['xnat_project', 'subject_id', 'exam_id', 'scan'],
                                              outfields=['dicoms']),
                     name=name)
    source.inputs.query_template = '/projects/%s/subjects/%s/experiments/%s/scans/%d/resources/files'
    # xnatStructSource.inputs.query_template = '/projects/%s/subjects/%s/experiments/%s/scans'
    source.inputs.query_template_args['dicoms'] = [['xnat_project', 'subject_id', 'exam_id', 'scan']]

    return source


def create_convert(name):
    ''' Use the Dicomstack Interface to store DICOM Header information into the
        extended NIfTI header.
        Output Format is unzipped n+1 (SPM-preferred).
    '''
    convert = pe.MapNode(interface=dcmstack.DcmStack(), name=name,
                         iterfield=['dicom_files'])
    convert.inputs.out_ext = '.nii'
    convert.inputs.embed_meta = True

    return convert


def create_cache(name):
    cacheSource = pe.Node(interface=util.Function(input_names=['cache_dir', 'exam_id'], output_names='exam_cache',
                                                  function=exam_cache_dir),
                          name='cacheSource', run_without_submitting=True)
    return cacheSource




# ##################################
#
#
#
# # CBS Central (XNAT) Workflows for Download and NIfTI conversion of DICOM Images
# # ekk 12Nov12
#
# import os
# import json
#
# # Import NiPype
# import nipype.interfaces.io as nio                  # Data i/o
# import nipype.interfaces.utility as util            # utility
# import nipype.pipeline.engine as pe                 # pypeline engine
# import nipype.interfaces.dcmstack as dcmstack       # NIfTI Conversion with DICOM Header
#
# from nipype import config
#
# from .utils import build_datasink
#
# import cbscentral_interfaces                        # Custom XNAT interfaces
#
# ''' XNAT
#    ================================================================================
#
#    Searching XNAT and pulling down data is handled by two distinct nodes.
#
# 1) xnatSearch takes an experiment ID and a descriptive pattern and searches for scans belonging to the experiment matching the pattern.
#    For example, 'mprage%RMS' for a T1, and returns that the experiment's 3'rd scan is the one to download.
#
# 2) Once the list of scans to pull in is known, xnatSource then handles downloading them to the proper cache directory and
#    making them available to dcm2nii or whatever dicom converter is being used.
# '''
#
class XnatServerConfigInterfaceInputSpec(BaseInterfaceInputSpec):
    config_file = traits.File(exists=True, desc='Credentials file')
    alias = traits.String(desc='Alias to look for in xml file', mandatory=True)


class XnatServerConfigInterfaceOutputSpec(TraitedSpec):
    user = traits.String(desc="Login")
    pwd = traits.String(desc="Password")
    server = traits.String(desc="Server address")
    config = traits.Dict(desc="Credentials as dict")

class XnatServerConfigInterface(BaseInterface):
    input_spec = XnatServerConfigInterfaceInputSpec
    output_spec = XnatServerConfigInterfaceOutputSpec

    def _run_interface(self, runtime):
        if self.inputs.config_file:
            config_file = self.inputs.config_file
        else:
            config_file = op.expanduser('~/.xnat_auth')

        alias = self._read_auth(config_file)
        self.config = dict(
                user=alias.findtext('username'),
                pwd=alias.findtext('password'),
                server=alias.findtext('url')
            )

        return runtime

    def _read_auth(self, config_file):
        dom = xml.etree.ElementTree.parse(config_file).getroot()
        return dom.findall(self.inputs.alias)[0]


    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs["user"] = self.config["user"]
        outputs["pwd"] = self.config["pwd"]
        outputs["server"] = self.config["server"]
        outputs["config"] = self.config
        return outputs


# XnatSearchInterface
#####################

class XnatSearchInterfaceInputSpec(BaseInterfaceInputSpec):
    session_label = traits.String(desc='Scan Session Label', mandatory=True)
    desc_pattern = traits.String(desc='Scan Series Label', mandatory=True)
    xnat_config = traits.Dict(mandatory=True,
        desc="Xnat Config")
    series_offset = traits.Int(0, usedefault=True, desc="series_offset")


class XnatSearchInterfaceOutputSpec(TraitedSpec):
    scans = traits.List(traits.Int(), desc="Scan Numbers to Retrieve")
    subject = traits.String(desc="Subject id tied to this scan")


class XnatSearchInterface(BaseInterface):
    input_spec = XnatSearchInterfaceInputSpec
    output_spec = XnatSearchInterfaceOutputSpec

    def _run_interface(self, runtime):
        session_label = self.inputs.session_label
        desc_pattern = self.inputs.desc_pattern
        xnat_config = self.inputs.xnat_config
        series_offset = self.inputs.series_offset

        self.scans, self.subject = self.xnat_search(session_label, desc_pattern,
                                      xnat_config, series_offset)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs["scans"] = self.scans
        outputs["subject"] = self.subject
        return outputs

    def xnat_search(self, session_label, series_pattern, xnat_config, series_offset = 0):
        from pyxnat import Interface

        central = Interface(server=xnat_config['server'],
                            user=xnat_config['user'],
                            password=xnat_config['pwd'])
        # session_pattern = '%%%s%%' % session_label.split('_')[0]  # Hackety for PrisonReward Subject/Experiment Label issues!!
        # session_pattern = '%%%s%%' % session_label
        session_pattern = session_label
        print session_pattern, series_pattern
        results = central.select(
                    'xnat:mrscandata',
                    ['xnat:mrSessionData/SUBJECT_ID', 'xnat:mrSessionData/LABEL', 'xnat:mrscandata/ID', 'xnat:mrscandata/QUALITY']
                    ).where([
                      ('xnat:mrscandata/SERIES_DESCRIPTION','LIKE', series_pattern),
                      ('xnat:mrSessionData/LABEL', 'LIKE', session_pattern),
                      ('xnat:mrScanData/QUALITY', '=', 'usable'),
                      'AND'])
        print results
        """[('120613_PrP004', '5', 'usable'),
            ('120613_PrP004', '14', 'usable'),
            ('120613_PrP004', '21', 'usable')]
        """
        # print results
        if len(results.items()) == 0:
            raise LookupError("No scans found using %s and %s" % (series_pattern, session_pattern))
        scans = [int(result[2]) + series_offset for result in results.items()]  # Cast each scan to an int since the search returns them as strings.
        subject = results.data[0]['xnat_mrsessiondata_subject_id']
        print subject
        return scans, subject

#
# def clean_list(list, ext='.dcm'):
#     return [item for item in list if item.endswith(ext)]
#
#
# def create_xnatrecon_workflow(name='xnatrecon', config={}, checkManualQc=False, include_nodes=[]):
#     """Create a preprocessing workflow that downloads functionals and structurals
#     from xnat and reconstructs them to niftis.
#
#     Example
#     -------
#
#     >>> xnat_recon = cbscentral_workflows.create_xnatrecon_workflow()
#     >>>
#     >>> xnat_recon.inputs.inputspec.xnat_config = xnat_config
#     >>> xnat_recon.inputs.inputspec.xnat_project = 'Som_Clocks'
#     >>> xnat_recon.inputs.inputspec.struct_pattern = '%MPRage%'
#     >>> xnat_recon.inputs.inputspec.func_pattern = '%run%'
#
#     >>> reconNipype.connect([(infosource, xnat_recon, [('subject_id', 'inputspec.subject_id')])])
#
#
#     Inputs::
#
#          inputspec.subject_id       : Subject ID (Used in XNAT Interface as the Session Label to search for)
#          inputspec.exam_id          : Exam ID (Used in XNAT Interface)
#          inputspec.xnat_config      : XNAT Server Config ({'user':'user', 'pwd':secret, 'server':'cbscentral.harvard.edu'})
#          inputspec.xnat_project     : XNAT Project Name ('Som_Clocks')
#          inputspec.struct_pattern   : Structural Regex ('%MPRage%')
#          inputspec.func_pattern     : Functional Regex ('%run%')
#          inputspec.func_offset      : Scan to increment (usually 0 or 1, meaning either the scan matched or plus 1 (for some derived scans.)
#          inpustspec.cache_dir       : Temp directory for downloading DICOMs from XNAT.
#
#     Inputs may be set by directly accessing the inputspec, or by passing in a config dictionary.
#
#     Outputs::
#
#          outputnode.func_images    : List of output functional 4d NIfTIs
#          outputnode.struct_image   : Structural NIfTI Image
#
#          If checkManualQc is True, also:
#
#          outputnode.scan_nums      : Series Numbers of Useful scans
#          outputnode.runs           : 0-indexed run numbers (for selecting behavioral logfiles)
#
#     """
#
#     workflow = pe.Workflow(name=name)
#
#     """
#     Define the inputs to this workflow
#     """
#
#
#
#
#
#
#
#     '''xnat Source Nodes handle the actual download and retrieval of image data from the XNAT Server.
#     Specifically, Structural and Functional images are downloaded into different local cache directories
#     since dcm2nii operates on a per-directory basis.'''
#     xnatStructSource = pe.Node(interface=nio.XNATSource(infields=['xnat_project', 'subject_id', 'exam_id', 'scan'], outfields=['dicoms']), name='xnatStructSource')
#     xnatStructSource.inputs.query_template = '/projects/%s/subjects/%s/experiments/%s/scans/%d/resources/files'
#     # xnatStructSource.inputs.query_template = '/projects/%s/subjects/%s/experiments/%s/scans'
#     xnatStructSource.inputs.query_template_args['dicoms'] = [['xnat_project', 'subject_id', 'exam_id', 'scan']]
#     '''
#     xnatStructSource.inputs.cache_dir = tempfile.mkdtemp('struct')  # Cache Dir set during inputs in workflow to incorporate subject_id
#     '''
#
#
#     xnatFuncSource = xnatStructSource.clone('xnatFuncSource')
#     '''
#     xnatFuncSource.inputs.cache_dir = tempfile.mkdtemp('func')  # Cache Dir set during inputs in workflow to incorporate subject_id
#     '''
#
#     ''' Use the new Dicomstack Interface to store DICOM Header information into the
#         extended NIfTI header.
#
#         Output Format is unzipped n+1 (SPM-preferred).
#     '''
#     convertStruct = pe.MapNode(interface=dcmstack.DcmStack(), name='convertStruct', iterfield=['dicom_files'])
#     convertStruct.inputs.out_ext = '.nii'
#     if 'embed_meta' in config.keys():
#         convertStruct.inputs.embed_meta = config['embed_meta']
#     # convertStruct.config = dict(execution={'remove_unnecessary_outputs':False})
#
#     # config.update_config(convertStruct.config)
#
#     # convertStruct.config['remove_unnecessary_outputs'] = False
#
#     convertFunc = pe.MapNode(interface=dcmstack.DcmStack(), name='convertFunc', iterfield=['dicom_files'])
#     convertFunc.inputs.out_ext = '.nii'
#     convertFunc.inputs.embed_meta = True
#     # convertStruct.config = dict(execution={'remove_unnecessary_outputs':False})
#
#     # convertFunc.config['remove_unnecessary_outputs'] = False
#
#     outputnode_fields = ['func_images', 'struct_image']
#
#     def select_dicoms(lists):
#         if not isinstance(lists[0], list):
#             lists = [lists]
#         # return [[item for item in l if item.endswith('dcm') or item.endswith('IMA')] for l in lists]
#         return lists
#
#     def clean_list(images_list):
#         flatten = lambda nested_list: [item for sublist in nested_list for item in sublist]
#         if isinstance(images_list[0], list):
#             dicoms = flatten(images_list)
#         else:
#             dicoms = images_list
#         return [item for item in dicoms if item.endswith('dcm') or item.endswith('IMA')]
#
#     get_val_for = lambda dict, key: dict[key]
#
#
#     # Define Nodes for Manual QC Checks
#     # =================================
#     if checkManualQc:
#         def get_qcQuery(exam_id,scan_ids=[]):
#             # scan_id is not a scan's uid, but more like series number.
#             # However, xnat calls it scan_id, so we're staying with xnat's
#             # query nomenclature for consistency.
#             query = [('neuroinfo:manualboldqc/EXPT_ID','LIKE','%s%%' % exam_id)]
#             scan_params = [('neuroinfo:manualboldqc/SCAN_ID','LIKE',str(id)) for id in scan_ids]
#             scan_params.append('OR')
#             query.append(scan_params)
#             query.append('AND')
#
#             # query = [('neuroinfo:manualboldqc/EXPT_ID','LIKE','%s%%' % exam_id),
#             #            [  ('neuroinfo:manualboldqc/SCAN_ID','LIKE','9'),
#             #               ('neuroinfo:manualboldqc/SCAN_ID','LIKE','12'),
#             #               'OR'],
#             #            'AND']
#
#             return query
#
#         build_qcQuery = pe.Node(interface=util.Function(input_names=['exam_id','scan_ids'],
#                                     output_names=['query'],
#                                     function=get_qcQuery),
#                                 name='build_qcQuery', run_without_submitting=True)
#
#         qcSearch = pe.Node(interface=cbscentral_interfaces.XnatQuery(), name='qcSearch')
#         qcSearch.inputs.query_datatype = 'neuroinfo:manualboldqc'
#
#         def choose_scans(scan_ids, scan_qcs, remove=['FAIL']):
#             """ Select Scans and Run Numbers/Orders of scans with passing Manual QC Checks
#             """
#             passing_scans = []
#             passing_orders = []
#             use_all = False if scan_qcs else True  # Use all scans if NO assessments were found
#             if use_all:
#                 passing_scans = scan_ids
#                 passing_orders = range(len(scan_ids))
#
#
#             for order, scan_id in enumerate(scan_ids):
#                 qc = scan_qcs.where(scan_id=str(scan_id))
#                 if qc.has_header('overall'):
#                     if qc['overall'] not in remove:
#                         passing_scans.append(scan_id)
#                         passing_orders.append(order)
#                         scan_msg = "%sed Manual QC - %s" % (qc['overall'].capitalize(), qc['comments'])
#                     else:
#                         scan_msg = "Failed Manual QC - %s\n%s" % (qc['comments'], qc.dumps_json())
#                 else:
#                     scan_msg = "No Manual Assessment found."
#                 print "Scan Series %s: %s" % (scan_id, scan_msg)
#
#             if not passing_scans:
#                 raise RuntimeError("No scans passed Manual QC.")
#             elif len(passing_scans) == 1:
#                 raise RuntimeError("Only 1 passing scan found. Aborting.")
#
#             return passing_scans, passing_orders
#
#         qcSelect = pe.Node(interface=util.Function(input_names=['scan_ids','scan_qcs'],
#                                                    output_names=['passing_scans','passing_orders'],
#                                                    function=choose_scans), name='qcSelect', run_without_submitting=True)
#         outputnode_fields.extend(['scan_nums','runs'])
#
#     outputnode = pe.Node(interface=util.IdentityInterface(fields=outputnode_fields), name='outputnode', run_without_submitting=True)
#
#     def exam_cache_dir(cache_dir,exam_id):
#         import os
#         return os.path.join(cache_dir,exam_id)
#
#     cacheSource = pe.Node(interface=util.Function(input_names=['cache_dir', 'exam_id'], output_names='exam_cache',
#                                                   function=exam_cache_dir),
#                             name='cacheSource', run_without_submitting=True)
#
#
#     # Don't Remove DICOMS After execution (for re-running flow)
#     workflow.config['execution'] = {'remove_unnecessary_outputs': False}
#     workflow.connect([
#                         (inputnode, xnatStructSearch, [('xnat_config', 'xnat_config'),
#                                                        ('struct_pattern', 'desc_pattern'),
#                                                        ('struct_offset', 'series_offset'),
#                                                        ('exam_id', 'session_label')]),
#                         (inputnode, xnatFuncSearch, [('xnat_config', 'xnat_config'),
#                                                       ('func_pattern', 'desc_pattern'),
#                                                       ('func_offset', 'series_offset'),
#                                                       ('exam_id', 'session_label')]),
#                         (inputnode, cacheSource,     [('cache_dir','cache_dir'),
#                                                       ('exam_id', 'exam_id')]),
#                         (cacheSource, xnatStructSource,[('exam_cache', 'cache_dir')]),
#                         (cacheSource, xnatFuncSource,  [('exam_cache', 'cache_dir')]),
#                         (inputnode, xnatStructSource, [('xnat_project', 'xnat_project'),
#                                                        ('subject_id', 'subject_id'),
#                                                        ('exam_id', 'exam_id'),
#                                                        (('xnat_config', get_val_for, 'user' ), 'user'),
#                                                        (('xnat_config', get_val_for, 'pwd' ), 'pwd'),
#                                                        (('xnat_config', get_val_for, 'server' ), 'server'),
#                                                       ]),
#                         (inputnode, xnatFuncSource, [('xnat_project', 'xnat_project'),
#                                                        ('subject_id', 'subject_id'),
#                                                        ('exam_id', 'exam_id'),
#                                                        (('xnat_config', get_val_for, 'user' ), 'user'),
#                                                        (('xnat_config', get_val_for, 'pwd' ), 'pwd'),
#                                                        (('xnat_config', get_val_for, 'server' ), 'server'),
#                                                       ]),
#                         (xnatStructSearch, xnatStructSource, [('scan','scan')]),
#                         (xnatStructSource, convertStruct, [(('dicoms', select_dicoms), 'dicom_files')]),
#                         (xnatFuncSource, convertFunc, [(('dicoms', select_dicoms), 'dicom_files')]),
#                         (convertStruct, outputnode, [('out_file', 'struct_image')]),
#                         (convertFunc, outputnode, [('out_file', 'func_images')])
#     ])
#
#     if checkManualQc:
#         workflow.connect([
#             (inputnode, qcSearch, [(('xnat_config', get_val_for, 'user' ), 'user'),
#                                    (('xnat_config', get_val_for, 'pwd' ), 'pwd'),
#                                    (('xnat_config', get_val_for, 'server' ), 'server')]),
#             (inputnode, build_qcQuery,      [('exam_id', 'exam_id')]),
#             (xnatFuncSearch, build_qcQuery, [('scan', 'scan_ids')]),
#             (build_qcQuery, qcSearch,       [('query', 'query')]),
#             (xnatFuncSearch, qcSelect,      [('scan', 'scan_ids')]),
#             (qcSearch, qcSelect,            [('result', 'scan_qcs')]),
#             (qcSelect, outputnode,          [('passing_scans', 'scan_nums'),
#                                              ('passing_orders', 'runs')]),
#             (qcSelect, xnatFuncSource,      [('passing_scans', 'scan')])
#         ])
#     else:
#         workflow.connect([
#             (xnatFuncSearch, xnatFuncSource, [('scan', 'scan')]),
#         ])
#
#     if 'sink' in include_nodes:
#         if 'sink_dir' in config.keys():
#             datasink = build_datasink(base_directory = config['sink_dir'])
#         else:
#             datasink = build_datasink()
#         workflow.connect([
#             (outputnode, datasink, [('struct_image','struct'),
#                                     ('func_images','func')]),
#         ])
#
#     return workflow
#
# def set_xnat_config(config):
#     conf_keys = config.keys()
#     # Set XNAT Authentication
#     # If given a config dict, use that. If given a path to an existing json, use that. Finally, use user/password.
#     if 'xnat_use_authfile' in conf_keys and config['xnat_use_authfile']:
#         if 'xnat_config' in conf_keys:
#             if isinstance(config['xnat_config'], dict):
#                 inputnode.inputs.xnat_config = config['xnat_config']
#             else:
#                 fullfile = os.path.abspath(os.path.expanduser(config['xnat_config']))
#                 if os.path.exists(fullfile):
#                     with open(fullfile, 'r') as f:
#                         xnat_config = json.load(f)
#                 else:
#                     raise RuntimeError('XNAT Auth file %s not found.' % fullfile)
#     else:
#         xnat_config = dict(
#             server=config['xnat_host'],
#             user=config['xnat_user'],
#             pwd=config['xnat_pass']
#         )
#     return xnat_config
#
#
# # def flatten_list(deep_list):
# #     """xnatSource, when run with an array of inputs, creates list of lists for each run.
# #     However, dcm2nii expects only a single list of files. This flattens the array.
# #     """
# #     import itertools
# #     depth = lambda L: isinstance(L, list) and max(map(depth, L))+1
# #     # http://stackoverflow.com/questions/6039103/counting-deepness
# #
# #     if depth(deep_list) == 1:
# #         flattened = deep_list  # If the list is already flat, just return it.
# #     else:
# #         # Otherwise, flatten it using itertools. This is necessary because flattening
# #         # a list of depth 1, will actually flatten it into a long array of single chars,
# #         # and the first item will therefore be a single character ('/'), not a full path.
# #         flattened = list(itertools.chain(*deep_list))
# #
# #     return flattened
#
# #!/usr/bin/env python
# '''Interface to query XNAT.'''
#
# from nipype.interfaces.base import BaseInterface, BaseInterfaceInputSpec, TraitedSpec, traits, InputMultiPath, OutputMultiPath, File, Bunch, Directory
# from nipype.utils.filemanip import split_filename
# import os
# import tempfile
# import pyxnat
#
# # import scipy.io
# # from numpy import transpose
#
# class XnatQueryInputSpec(BaseInterfaceInputSpec):
#     # query_template = traits.Str(
#     #     mandatory=True,
#     #     desc=('Layout used to build query.')
#     # )
#     #
#     # query_template_args = traits.Str(desc='Single arg for the moment.')
#
#     query = traits.List(desc='Full query to search for.', mandatory=True)
#
#     query_datatype = traits.Str(desc='xnat datatype to return.', mandatory=True)
#     # traits.Dict(
#     #         traits.Str,
#     #         traits.List(traits.List),
#     #         value=dict(outfiles=[]), usedefault=True,
#     #         desc='Information to plug into template'
#     #     )
#
#     server = traits.Str(
#         mandatory=True,
#         requires=['user', 'pwd'],
#         xor=['config']
#     )
#
#     user = traits.Str()
#     pwd = traits.Password()
#     config = File(mandatory=True, xor=['server'])
#
#     cache_dir = Directory(desc='Cache directory')
#
# class XnatQueryOutputSpec(TraitedSpec):
#     result = traits.Any(desc='JsonTable returned from XNAT', mandatory=True)
#
# class XnatQuery(BaseInterface):
#     """ Query XNAT and return json table result.
#         Note - this will warn but won't raise an error if nothing was found -
#         it's quite possible that some queries will come back blank
#         (ie looking for manual QC when none has been created.)
#     """
#     input_spec = XnatQueryInputSpec
#     output_spec = XnatQueryOutputSpec
#
#     def _run_interface(self, runtime):
#         cache_dir = self.inputs.cache_dir or tempfile.gettempdir()
#
#         if self.inputs.config:
#             xnat = pyxnat.Interface(config=self.inputs.config)
#         else:
#             xnat = pyxnat.Interface(self.inputs.server,
#                                     self.inputs.user,
#                                     self.inputs.pwd,
#                                     cache_dir
#                                     )
#         dtype = self.inputs.query_datatype
#         query = self.inputs.query
#
#         print "Searching XNAT with: %s" % query
#
#         result = xnat.select(dtype).where(query)
#         if not result:
#             print 'Warning! No results returned!'
#         # print result
#         self.result = result
#
#         return runtime
#
#     def _list_outputs(self):
#         outputs = self._outputs().get()
#         outputs['result'] = self.result
#         return outputs

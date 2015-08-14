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


default_parameters = dict(
    offsets=None,
    series_descriptions=[],
    server=None,
    out_ext='.nii',
    sanitize_wildcard=True,
    cache_dir='/scratch/DICOM',
)


def workflow_manager(project, exp, args, subj_source):
    """
    # ------------------------------------------- #
    # Xnat Download and Nifti Conversion Workflow #
    # ------------------------------------------- #

    Wrapped with Data Source and Sink
    """

    # Create workflow in function defined elsewhere in this package
    (xnatconvert, xnatconvert_input,
        xnatconvert_output) = workflow_spec(exp_info=exp)

    xnatconvert.connect([
        (subj_source, xnatconvert_input, [('subject_id', 'subject_id')])
    ])

    xnatconvert_sink = Node(DataSink(base_directory=project['data_dir']),
                            "xnatconvert_sink")

    # Similar to above, class to handle sterotyped output connections
    xnatconvert_outwrap = fitz.tools.OutputWrapper(
        xnatconvert, subj_source, xnatconvert_sink, xnatconvert_output)
    xnatconvert_outwrap.set_subject_container()
    xnatconvert_outwrap.add_regexp_substitutions(("_convert\d+\/", ""))
    xnatconvert_outwrap.sink_outputs("images")

    # Set the base for the possibly temporary working directory
    xnatconvert.base_dir = project['working_dir']

    return xnatconvert


def workflow_spec(name="xnatconvert", exp_info=None):
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

         outputnode.images    : List of output image NIfTIs
    """
    if exp_info is None:
        exp_info = fitz.default_experiment_parameters

    # Define the inputs for the preprocessing workflow
    in_fields = ['subject_id',
                 'session_label',
                 'xnat_config',
                 'xnat_project',
                 'subject_id',
                 'exam_id',
                 'series_descriptions',
                 'offsets',
                 'cache_dir',
                 'server',
                 'server_alias'
                 ]

    inputnode = Node(IdentityInterface(in_fields), "inputs")
    inputnode.inputs.xnat_project = exp_info['xnat_project']
    inputnode.inputs.series_descriptions = exp_info['series_descriptions']

    # If no offsets were set in exp_info, update them to 0s matching
    # the length of series_descriptions.
    if exp_info['offsets']:
        offsets = exp_info['offsets']
    else:
        offsets = [0 for i in range(len(exp_info['series_descriptions']))]
    inputnode.inputs.offsets = offsets

    inputnode.inputs.server_alias = exp_info['server_alias']

    pattern_flow = create_xnatconvert_flow(exp_info=exp_info,
                                           name='xnatconvert_subflow')

    xnatconvert_subject = Workflow(name)
    xnatconvert_subject.connect([
        (inputnode, pattern_flow,
            [('series_descriptions', 'xnatSearch.desc_pattern'),
             ('offsets', 'xnatSearch.series_offset'),
             ('subject_id', 'xnatSearch.session_label'),
             ('xnat_project', 'xnatSource.xnat_project'),
             ('server_alias', 'xnatServerConfig.alias'),
             ]),
    ])

    # Define the outputs of the top-level workflow
    output_fields = ["images"]
    outputnode = Node(IdentityInterface(output_fields), "outputs")

    xnatconvert_subject.connect([
        (pattern_flow, outputnode,
            [("convert.out_file", "images")]),
        # (func_flow, outputnode,
        #     [("convert.out_file", "func_images")]),
    ])

    return xnatconvert_subject, inputnode, outputnode


def first_item(l):
    if isinstance(l, list):
        return l[0]
    else:
        return l


def flatten_list(nested_list):
    return [item for sublist in nested_list for item in sublist]


def create_xnatconvert_flow(exp_info, name='xnatconvert_flow'):
    xnatIdent = create_serverconfig(name='xnatServerConfig')
    xnatSearch = create_search(exp_info, name='xnatSearch')
    xnatSource = create_source(exp_info, name='xnatSource')
    convert = create_convert(exp_info, name='convert')

    workflow = pe.Workflow(name=name)
    workflow.connect([
        (xnatIdent, xnatSearch,
            [('config', 'xnat_config')]),
        (xnatIdent, xnatSource,
            [('user', 'user'),
             ('pwd', 'pwd'),
             ('server', 'server')]),
        (xnatSearch, xnatSource,
            [(('scans', flatten_list), 'scan'),
             (('subject', first_item), 'subject_id'),
             (('exam', first_item), 'exam_id')]),
        (xnatSource, convert,
            [(('dicoms', select_dicoms), 'dicom_files')]),
    ])

    return workflow


def select_dicoms(lists):
    if not isinstance(lists[0], list):
        lists = [lists]
    return lists


def clean_list(images_list):
    def flatten(nested_list):
        return [item for sublist in nested_list for item in sublist]
    if isinstance(images_list[0], list):
        dicoms = flatten(images_list)
    else:
        dicoms = images_list
    return [item for item in dicoms
            if item.endswith('dcm') or item.endswith('IMA')]


def get_val_for(d, key):
    return d[key]


def exam_cache_dir(cache_dir, exam_id):
    import os
    return os.path.join(cache_dir, exam_id)


def create_serverconfig(name):
    config = pe.Node(interface=XnatServerConfigInterface(), name=name)
    return config


def create_search(exp_info, name):
    search = pe.MapNode(interface=XnatSearchInterface(),
                        iterfield=['desc_pattern', 'series_offset'],
                        name=name)
    search.inputs.sanitize_wildcard = exp_info['sanitize_wildcard']
    return search


def create_source(exp_info, name):
    source = pe.Node(interface=nio.XNATSource(
            infields=['xnat_project', 'subject_id', 'exam_id', 'scan'],
            outfields=['dicoms']),
        name=name)
    source.inputs.query_template = ('/projects/%s/subjects/%s/experiments/' +
                                    '%s/scans/%d/resources/files')
    source.inputs.query_template_args['dicoms'] = [[
        'xnat_project', 'subject_id', 'exam_id', 'scan']]
    source.inputs.cache_dir = exp_info['cache_dir']
    return source


def create_convert(exp_info, name):
    ''' Use the Dicomstack Interface to store DICOM Header information into the
        extended NIfTI header.
        Output Format is unzipped n+1 (SPM-preferred).
    '''
    convert = pe.MapNode(interface=dcmstack.DcmStack(), name=name,
                         iterfield=['dicom_files'])
    convert.inputs.out_ext = exp_info['out_ext']
    convert.inputs.embed_meta = True

    return convert


def create_cache(name):
    cacheSource = pe.Node(interface=util.Function(
            input_names=['cache_dir', 'exam_id'], output_names='exam_cache',
            function=exam_cache_dir),
        name='cacheSource', run_without_submitting=True)
    return cacheSource


class XnatServerConfigInterfaceInputSpec(BaseInterfaceInputSpec):
    config_file = traits.File(exists=True, desc='Credentials file')
    alias = traits.String(desc='Server Alias Name', mandatory=True)


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
    xnat_config = traits.Dict(mandatory=True, desc="Xnat Config")
    series_offset = traits.Int(0, usedefault=True, desc="series_offset")
    sanitize_wildcard = traits.Bool(True, usedefault=True)


class XnatSearchInterfaceOutputSpec(TraitedSpec):
    scans = traits.List(traits.Int(), desc="Scan Numbers to Retrieve")
    subject = traits.String(desc="Xnat subject id")
    exam = traits.String(desc="Xnat exam id")


class XnatSearchInterface(BaseInterface):
    input_spec = XnatSearchInterfaceInputSpec
    output_spec = XnatSearchInterfaceOutputSpec

    def _run_interface(self, runtime):
        session_label = self.inputs.session_label
        desc_pattern = self.inputs.desc_pattern
        xnat_config = self.inputs.xnat_config
        series_offset = self.inputs.series_offset

        self.scans, self.subject, self.exam = self.xnat_search(
            session_label, desc_pattern, xnat_config, series_offset)
        return runtime

    def _list_outputs(self):
        outputs = self._outputs().get()
        outputs["scans"] = self.scans
        outputs["subject"] = self.subject
        outputs["exam"] = self.exam
        return outputs

    def xnat_search(self, session_label, series_label, xnat_config,
                    series_offset=0):
        from pyxnat import Interface

        central = Interface(server=xnat_config['server'],
                            user=xnat_config['user'],
                            password=xnat_config['pwd'])

        if self.inputs.sanitize_wildcard:
            session_pattern = '%%%s%%' % session_label.replace('*', '%')
            series_pattern = '%%%s%%' % series_label.replace('*', '%')
        else:
            session_pattern = session_label
            series_pattern = series_label

        results = central.select(
            'xnat:mrscandata',
            ['xnat:mrSessionData/SUBJECT_ID', 'xnat:mrSessionData/SESSION_ID',
             'xnat:mrSessionData/LABEL',
             'xnat:mrscandata/ID', 'xnat:mrscandata/QUALITY']
            ).where([
              ('xnat:mrscandata/SERIES_DESCRIPTION', 'LIKE', series_pattern),
              ('xnat:mrSessionData/LABEL', 'LIKE', session_pattern),
              ('xnat:mrScanData/QUALITY', '=', 'usable'),
              'AND'])

        """[('S04272', 'E01333', 'M87115017', '14', 'usable'),
            ('S04272', 'E01333', 'M87115017', '15', 'usable')]
        """
        if len(results.items()) == 0:
            raise LookupError("No scans found using %s and %s" % (
                series_pattern, session_pattern))
        # Cast each scan to an int since the search returns them as strings.
        scans = [int(result[3]) + series_offset for result in results.items()]
        subject = results.data[0]['xnat_mrsessiondata_subject_id']
        exam = results.data[0]['xnat_mrsessiondata_session_id']
        return scans, subject, exam

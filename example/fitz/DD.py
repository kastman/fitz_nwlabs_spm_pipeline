# Workflow Parameters
# --------------------
workflow = "nwlabs_workflows"
workflow_src = "git@ncfgit.rc.fas.harvard.edu:kastman/nwlabs_fitz.git"
workflow_version = "0.0.1-dev"

# Xnat Download and Convert
# --------------------------
xnat_project = 'Buckholtz_RSA'
struct_pattern = 'mprage%RMS'
func_pattern = 'ddt%'
server = 'https://cbscentral.rc.fas.harvard.edu'
server_alias = 'cbscentral'

# Preproc Parameters
# -------------------
func_template = "{subject_id}/images/*dd*"
anat_template = "{subject_id}/images/*mprage*"

n_runs = 3
TR = 2.5
temporal_interp = True
interleaved = False
slice_order = 'up'
num_slices = 33
smooth_fwhm = 6
hpcutoff = 120
frames_to_toss = 0

bases = {'hrf': {'derivs': [0, 0]}}
estimation_method = 'Classical'

# Default Model Parameters
# -------------------------
input_units = output_units = 'secs'

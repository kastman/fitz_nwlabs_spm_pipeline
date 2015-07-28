.. _experiments:

Defining Experimental Details
==============================

General parameters for directories and some file handling options are
stored in the project.py file, located in the FITZ_DIR.

Parameters specific to processing are stored in one or more experiment files.

Specific parameters that control the processing for each experiment are plain
python files stored in the FITZ_DIR called ``<experiment_name>.py``.

There is a default set of parameters that will be overridden by the fields in
this file; to see these defaults, run the following code::

    In [1]: import fitz

    In [2]: print fitz.default_experiment_parameters

Preprocessing Parameters
~~~~~~~~~~~~~~~~~~~~~~~~

.. glossary::

   source_template
    A string that, relative to your *data directory*, provides a path to the
    raw functional files for your experiment. It should include the string
    formatting key ``{subject_id}`` in place of the subject id in the path.
    When you have multiple runs of data, replace the run number with a glob
    wildcard. For instance, your template might look like
    ``"{subject_id}/bold/func_*.nii.gz"``.

   anat_template
    A string that, relative to your *data directory*, provides a path to the
    raw anatomical files for your experiment. It should include the string
    formatting key ``{subject_id`` in place of the subject id in the path.
    e.g.  ``anat_template = "{subject_id}/anatomy/highres001.img"``


   bases
    A dictionary compatible with the `nipype SPM model`_ specifying the basis
    functions used in the model, with one
    key that's one of 'hrf', 'fourier', 'fourier_han', 'gamma' or 'fir'
    and with values which are a dictionary of options for the chosen bases.

    Default: **{'hrf': {'derivs': [0,0]}}**

   estimation_method
    A string that's either 'Classical', 'Bayesian' or 'Bayesian2'. See the
    `nipype model estimation`_ for more detail.

.. _nipype SPM model: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.spm.model.html#level1design
.. _nipype model estimation: http://nipy.sourceforge.net/nipype/interfaces/generated/nipype.interfaces.spm.model.html#estimatemodel

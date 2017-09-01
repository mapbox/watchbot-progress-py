
class JobDoesNotExist(RuntimeError):
    """The reduce mode job doesn't exist (set_total has not been run). """


class JobFailed(RuntimeError):
    """Skip, the reduce mode job has already been marked as failed. """


class ProgressTypeError(TypeError):
    """Progress argument is not of the correct type"""

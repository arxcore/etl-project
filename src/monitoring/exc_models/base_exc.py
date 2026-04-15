class PipelineCrash(Exception):
    """Root Base Exceeption"""

    pass


class ProcessingFailed(PipelineCrash):
    """Base class to catch Failed of Extrak-Tranform-Format Data"""

    pass


class RoutingError(ProcessingFailed):
    """catch Routing Process Error"""

    pass


class FetchError(RoutingError):
    """catch Fetching process Error"""

    pass


class BLSError(FetchError):
    pass


class BEAError(FetchError):
    pass


class FREDError(FetchError):
    pass


class ParseError(RoutingError):
    """catch Parsing process Error"""

    pass


class BLSParseError(ParseError):
    pass


class BEAParseError(ParseError):
    pass


class FREDParseError(ParseError):
    pass


class StandardizedError(ProcessingFailed):
    """catch Error Standardizer data"""

    pass


class FilterError(ProcessingFailed):
    """catch Error Filters data"""

    pass


class CalculateError(ProcessingFailed):
    """catch Error Calculation data"""

    pass


class FormatError(ProcessingFailed):
    """catch Error Format Final data"""

    pass


class UploadFailed(PipelineCrash):
    """catch Error Upload to DB"""

    pass


class ResourceNotFound(PipelineCrash):
    """catch Error Load Resource .env File"""

    pass

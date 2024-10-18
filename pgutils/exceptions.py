class QueryValidationError(Exception):
    """Exception for invalid queries."""
    pass

class ExcessiveSelectWarning(Warning):
    """Warning raised for the use of SELECT * in SQL queries."""
    pass

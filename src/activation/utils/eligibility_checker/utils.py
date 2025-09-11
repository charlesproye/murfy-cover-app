import functools
import inspect
import logging

logger = logging.getLogger("VIN Size checker")


def require_valid_vins(func):
    """Decorator that checks all VINs passed to the function are exactly 17 characters long."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        # Get the function signature to identify the `vins` argument
        sig = inspect.signature(func)
        bound_args = sig.bind(*args, **kwargs)
        bound_args.apply_defaults()

        vins = bound_args.arguments.get("vins")
        if vins is None:
            raise ValueError("Missing 'vins' argument for the decorated function")

        # Validate VINs length
        invalid_vins = [vin for vin in vins if len(vin) != 17]
        if invalid_vins:
            logger.error(f"Execution of {func.__name__} failed - Those vins are not 17 chars long: {invalid_vins};")
            raise ValueError(f"Invalid VINs (must be 17 chars): {invalid_vins}")

        return await func(*args, **kwargs)
    return wrapper

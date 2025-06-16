"""Facade to simplify project setup that calls project main function with kwargs"""

import logging
import sys
from inspect import getdoc
from typing import Any, Callable, Optional  # noqa: F401

import defopt
from makefun import with_signature

from hdx.api import __version__
from hdx.api.configuration import Configuration
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.useragent import UserAgent

logger = logging.getLogger(__name__)
setup_logging(log_file="errors.log")


def facade(projectmainfn: Callable[[Any], None], **kwargs: Any):
    """Facade to simplify project setup that calls project main function. It infers
    command line arguments from the passed in function using defopt. The function passed
    in should have type hints and a docstring from which to infer the command line
    arguments. Any **kwargs given will be merged with command line arguments, with the
    command line arguments taking precedence.

    Args:
        projectmainfn ((Any) -> None): main function of project
        **kwargs: Configuration parameters to pass to HDX Configuration & other parameters to pass to main function

    Returns:
        None
    """

    #
    # Setting up configuration
    #

    parsed_main_doc = defopt._parse_docstring(getdoc(projectmainfn))
    main_doc = [f"{parsed_main_doc.doc}\n\nArgs:"]
    no_main_parameters = len(parsed_main_doc.parameters)
    for param_name, param_info in parsed_main_doc.parameters.items():
        main_doc.append(
            f"\n    {param_name} ({param_info.annotation}): {param_info.doc}"
        )
    create_config_doc = getdoc(Configuration.create)
    kwargs_index = create_config_doc.index("**kwargs")
    kwargs_index = create_config_doc.index("\n", kwargs_index)
    args_doc = create_config_doc[kwargs_index:]
    main_doc.append(args_doc)
    main_doc = "".join(main_doc)

    main_sig = defopt.signature(projectmainfn)
    param_names = []
    for param in main_sig.parameters.values():
        param_names.append(str(param))

    parsed_main_doc = defopt._parse_docstring(main_doc)
    main_doc = [f"{parsed_main_doc.doc}Args:"]
    count = 0
    for param_name, param_info in parsed_main_doc.parameters.items():
        param_type = param_info.annotation
        if param_type == "dict":
            continue
        if count < no_main_parameters:
            count += 1
        else:
            if param_type == "str":
                param_type = "Optional[str]"
                default = None
            elif param_type == "bool":
                default = False
            else:
                raise ValueError(
                    "Configuration.create has new parameter with unknown type!"
                )
            param_names.append(f"{param_name}: {param_type} = {default}")
        main_doc.append(f"\n    {param_name} ({param_type}): {param_info.doc}")
    main_doc = "".join(main_doc)

    projectmainname = projectmainfn.__name__
    main_sig = f"{projectmainname}({','.join(param_names)})"

    argv = sys.argv[1:]
    for key in kwargs:
        name = f"--{key.replace('_', '-')}"
        if name not in argv:
            argv.append(name)
            argv.append(kwargs[key])

    @with_signature(main_sig)
    def gen_func(*args, **kwargs):
        """docstring"""
        site_url = Configuration._create(*args, **kwargs)
        logger.info("--------------------------------------------------")
        logger.info(f"> Using HDX Python API Library {__version__}")
        logger.info(f"> HDX Site: {site_url}")

    gen_func.__doc__ = main_doc

    configuration_create = defopt.bind(gen_func, argv=argv, cli_options="all")
    main_func, _ = defopt.bind_known(projectmainfn, argv=argv, cli_options="all")

    configuration_create()
    UserAgent.user_agent = Configuration.read().user_agent
    main_func()

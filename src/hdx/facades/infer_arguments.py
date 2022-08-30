"""Facade to simplify project setup that calls project main function with kwargs"""
import logging
import sys
from inspect import getdoc, signature
from typing import Any, Callable, Optional

import defopt
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.useragent import UserAgent
from makefun import with_signature

from hdx.api import __version__
from hdx.api.configuration import Configuration

logger = logging.getLogger(__name__)
setup_logging(log_file="errors.log")


def _create_configuration(
    user_agent: Optional[str] = None,
    user_agent_config_yaml: Optional[str] = None,
    user_agent_lookup: Optional[str] = None,
    hdx_url: Optional[str] = None,
    hdx_site: Optional[str] = None,
    hdx_read_only: bool = False,
    hdx_key: Optional[str] = None,
    hdx_config_json: Optional[str] = None,
    hdx_config_yaml: Optional[str] = None,
    project_config_json: Optional[str] = None,
    project_config_yaml: Optional[str] = None,
    hdx_base_config_json: Optional[str] = None,
    hdx_base_config_yaml: Optional[str] = None,
) -> str:
    """
    Create HDX configuration

    Args:
        user_agent (Optional[str]): User agent string. HDXPythonLibrary/X.X.X- is prefixed. Must be supplied if remoteckan is not.
        user_agent_config_yaml (Optional[str]): Path to YAML user agent configuration. Ignored if user_agent supplied. Defaults to ~/.useragent.yml.
        user_agent_lookup (Optional[str]): Lookup key for YAML. Ignored if user_agent supplied.
        hdx_url (Optional[str]): HDX url to use. Overrides hdx_site.
        hdx_site (Optional[str]): HDX site to use eg. prod, test.
        hdx_read_only (bool): Whether to access HDX in read only mode. Defaults to False.
        hdx_key (Optional[str]): Your HDX key. Ignored if hdx_read_only = True.
        hdx_config_json (Optional[str]): Path to JSON HDX configuration OR
        hdx_config_yaml (Optional[str]): Path to YAML HDX configuration
        project_config_json (Optional[str]): Path to JSON Project configuration OR
        project_config_yaml (Optional[str]): Path to YAML Project configuration
        hdx_base_config_json (Optional[str]): Path to JSON HDX base configuration OR
        hdx_base_config_yaml (Optional[str]): Path to YAML HDX base configuration. Defaults to library's internal hdx_base_configuration.yml.

    Returns:
        str: HDX site url

    """
    arguments = locals()
    return Configuration._create(**arguments)


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

    create_config_sig = signature(_create_configuration)
    create_config_params = list(create_config_sig.parameters.values())
    main_sig = signature(projectmainfn)
    main_params = list(main_sig.parameters.values())
    main_params.extend(create_config_params)
    main_sig = main_sig.replace(parameters=main_params)

    create_config_doc = getdoc(_create_configuration)
    parsed_main_doc = defopt._parse_docstring(getdoc(projectmainfn))
    main_doc = [f"{parsed_main_doc.first_line}\n\nArgs:"]
    for param_name, param_info in parsed_main_doc.params.items():
        main_doc.append(
            f"\n    {param_name} ({param_info.type}): {param_info.text}"
        )
    args_index = create_config_doc.index("Args:")
    args_doc = create_config_doc[args_index + 5 :]
    main_doc.append(args_doc)
    main_doc = "".join(main_doc)

    @with_signature(main_sig, func_name=projectmainfn.__name__)
    def gen_func(*args, **kwargs):
        """docstring"""
        return args, kwargs

    gen_func.__doc__ = main_doc

    argv = sys.argv[1:]
    for key in kwargs:
        name = f"--{key.replace('_', '-')}"
        if name not in argv:
            argv.append(name)
            argv.append(kwargs[key])

    defopt.bind(gen_func, argv=argv, cli_options="all")
    func, argv = defopt.bind_known(projectmainfn, argv=argv, cli_options="all")
    site_url = defopt.run(_create_configuration, argv=argv, cli_options="all")

    logger.info("--------------------------------------------------")
    logger.info(f"> Using HDX Python API Library {__version__}")
    logger.info(f"> HDX Site: {site_url}")

    UserAgent.user_agent = Configuration.read().user_agent
    func()

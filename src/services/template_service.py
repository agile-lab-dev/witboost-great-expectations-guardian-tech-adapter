from typing import Any

from jinja2 import (
    Environment,
    PackageLoader,
    Template,
    TemplateNotFound,
    select_autoescape,
)

from src.utility.logger import get_logger


class TemplateServiceError:
    def __init__(self, error_msg: str):
        self.error_msg = error_msg


class TemplateService:
    def __init__(self):
        self.logger = get_logger(__name__)

    def _get_template(self, technology: str) -> Template:
        env = Environment(loader=PackageLoader("src"), autoescape=select_autoescape())
        return env.get_template(f"{technology.casefold()}.jinja")

    def render_template(
        self, technology: str, parameters: dict[str, Any]
    ) -> str | TemplateServiceError:
        try:
            template = self._get_template(technology)
            return template.render(parameters)
        except TemplateNotFound:
            error_msg = f"Template not found for technology '{technology}'"
            self.logger.exception(error_msg)
            return TemplateServiceError(error_msg)
        except Exception as ex:
            error_msg = f"An error occurred while rendering the DAG for technology '{technology}'. Please try again later. Details: {str(ex)}"  # noqa: E501
            self.logger.exception(error_msg)
            return TemplateServiceError(error_msg)

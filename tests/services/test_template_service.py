from src.services.template_service import TemplateService, TemplateServiceError


def test_render_template_ok():
    template_service = TemplateService()
    technology = "Snowflake"

    res = template_service.render_template(technology, dict())

    assert isinstance(res, str)


def test_render_template_ko():
    template_service = TemplateService()
    technology = "unsupported"

    res = template_service.render_template(technology, dict())

    assert isinstance(res, TemplateServiceError)

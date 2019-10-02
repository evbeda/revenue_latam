from django import template
import stringcase

register = template.Library()


@register.filter(name='title')
def string_to_title(value):
    return stringcase.titlecase(value)

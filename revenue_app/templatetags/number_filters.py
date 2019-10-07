from django import template

register = template.Library()


@register.filter(name='is_numeric')
def is_numeric(value):
    return type(value).__name__ in ['int64', 'float64', 'int', 'float']

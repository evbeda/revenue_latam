from calendar import monthrange
from django import template
from datetime import date, timedelta

register = template.Library()


@register.filter(name='week_start')
def substract_date(value):
    return (value - timedelta(days=6))


@register.filter(name='semimonth_end')
def get_semimonth_end(value):
    if value.day < 15:
        end_day = 14
    else:
        end_day = monthrange(value.year, value.month)[1]
    return date(value.year, value.month, end_day)


@register.filter(name='quarter')
def get_quarter(value):
    return (value.month - 1)//3 + 1


@register.filter(name='quarter_start')
def get_quarter_start(value):
    return date(value.year, (value.month - 1) // 3 * 3 + 1, 1)

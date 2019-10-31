from django import forms
from datetime import date, timedelta


class CustomDateInput(forms.DateInput):
    input_type = 'date'


class QueryForm(forms.Form):
    okta_username = forms.CharField(widget=forms.TextInput())
    okta_password = forms.CharField(widget=forms.PasswordInput(render_value=True))
    start_date = forms.DateField(widget=CustomDateInput())
    end_date = forms.DateField(widget=CustomDateInput())

    def clean(self):
        cleaned_data = super().clean()
        start_date = cleaned_data.get('start_date')
        end_date = cleaned_data.get('end_date')

        if start_date and end_date:
            if end_date > date.today():
                error = forms.ValidationError("End date can't be greater than today.")
                self.add_error('end_date', error)

            if start_date > date.today():
                error = forms.ValidationError("Start date can't be greater than today.")
                self.add_error('start_date', error)

            if end_date < start_date:
                error = forms.ValidationError('End date must be greater or equal than start date.')
                self.add_error('start_date', error)
                self.add_error('end_date', error)

            if (end_date - start_date) > timedelta(days=92):
                error = forms.ValidationError("Time between End and Start date can't be over 3 months.")
                self.add_error('start_date', error)
                self.add_error('end_date', error)


class ExchangeForm(forms.Form):
    ars_to_usd = forms.FloatField(label='ARS to USD', required=True, min_value=0.01)
    brl_to_usd = forms.FloatField(label='BRL to USD', required=True, min_value=0.01)

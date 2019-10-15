from django import forms


class CustomDateInput(forms.DateInput):
    input_type = 'date'


class QueryForm(forms.Form):
    okta_username = forms.CharField(widget=forms.TextInput(attrs={'class': 'form-control'}))
    okta_password = forms.CharField(widget=forms.PasswordInput(render_value=True, attrs={'class': 'form-control'}))
    start_date = forms.DateField(widget=CustomDateInput(attrs={'class': 'form-control'}))
    end_date = forms.DateField(widget=CustomDateInput(attrs={'class': 'form-control'}))

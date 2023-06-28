from django import forms

class MovieForm(forms.Form):
    query = forms.CharField(label="Enter movie name here", max_length=200)

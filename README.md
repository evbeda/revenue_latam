# revenue_latam

[![Build Status](https://travis-ci.com/evbeda/revenue_latam.svg?branch=master)](https://travis-ci.com/evbeda/revenue_latam)

[![Coverage Status](https://coveralls.io/repos/github/evbeda/revenue_latam/badge.svg)](https://coveralls.io/github/evbeda/revenue_latam)

This project is about the sales and refunds in the latam market (Argentina and Brazil).

The main goal of this project is the capability to see the whole data summarized in graphics and tables to
 help the end user to make decisions.


### Prerequisites

* You have to have installed [python3](https://www.python.org/downloads/) (it is prefereble to install python3.7).
* git should be installed too.

#### For OS X users

If you do not have git do:

    $ brew install git

If you don't have installed brew, please follow this [tutorial](https://docs.brew.sh/Installation)

### Usage

Open up your terminal and download this project with:

    $ git clone https://github.com/evbeda/revenue_latam

Create a virutal environment:

    $ cd revenue_latam
    $ python3 -m venv env

Activate virtual environment and install project dependencies:

    $ source env/bin/activate
    $ pip install -r requirements.txt

Export the necessary environment variables

Execute the project with:

    $ python manage.py runserver

Finally open up your browser and type http://127.0.0.1:8000/ in your address bar.

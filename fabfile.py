from fabric.api import *

import os


@task
def install_postgres():
    run('echo deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main >> /etc/apt/sources.list.d/pgdg.list')
    run('wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -')
    run('apt-get update')
    run('apt-get install postgresql-9.4')

    try:
        sudo("""
    su postgres sh -c "echo CREATE ROLE collector UNENCRYPTED PASSWORD \\'umut9273\\' NOSUPERUSER CREATEDB NOCREATEROLE INHERIT LOGIN\\; | psql"
    """)
    except Exception as e:
        print '----'
        print e
        print '----'

    try:
        sudo("""
    su postgres sh -c "echo CREATE DATABASE \\"btc\\" WITH OWNER \\"collector\\" ENCODING \\'UTF8\\' LC_COLLATE = \\'en_US.UTF-8\\' LC_CTYPE = \\'en_US.UTF-8\\' template template0\\; | psql"
    """)
    except Exception as e:
        print '----'
        print e
        print '----'

#!/usr/bin/python
# -*- coding: utf-8 -*-

"""

Ansible module to run SQL requests and files to a running MySQL instance
(c) 2016, Loic Blot <loic.blot@unix-experience.fr>
Sponsored by Infopro Digital. http://www.infopro-digital.com/
Sponsored by E.T.A.I. http://www.etai.fr/

This file is part of Ansible

Ansible is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

Ansible is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with Ansible.  If not, see <http://www.gnu.org/licenses/>.
"""

DOCUMENTATION = '''
---
module: mysql_query

short_description: Execute SQL requests
description:
    - Execute a single SQL query or single SQL file on a MySQL server
version_added: "2.1"
author: "Loic Blot (@nerzhul)"
options:
    db:
        description:
            - MySQL database where queries will be executed
        default: mysql
    query:
        description:
            - SQL query to execute
        default: None
    query_file:
        description:
            - SQL filepath to execute
        default: None

extends_documentation_fragment: mysql
'''
EXAMPLES = '''
# Execute a single query
- mysql_query: db=concepts query="INSERT INTO `amazing_things` (id,description) VALUES (1,'test');"

# Execute a SQL file
- mysql_query: db=cars query_file=/usr/share/cars/0001_initdb.sql
'''

RETURN = '''
changed:
    description: If module has executed a query
    returned: success
    type: string
rows_affected:
    description: number of rows affected by the query or query_file
    returned: success
    type: int
'''

import os
import warnings
from re import match

try:
    import MySQLdb
except ImportError:
    mysqldb_found = False
else:
    mysqldb_found = True


def execute_query(cursor, query, check_mode):
    # execute a specific query
    try:
        cursor.execute(query)
        if check_mode:
            cursor.connection.rollback()
        else:
            cursor.connection.commit()
        result = (True, cursor.rowcount)
    except Exception, e:
        result = (str(e), 0)
    return result


def execute_query_file(cursor, query_file, check_mode):
    try:
        row_affected = 0
        for line in open(query_file):
            # Ignore empty lines and comments
            if re.match("^\s*$", line) or re.match("^\s*-{2,}", line):
                continue
            cursor.execute(line)
            row_affected += cursor.rowcount
        if check_mode:
            cursor.connection.rollback()
        else:
            cursor.connection.commit()
        result = (True, row_affected)
    except Exception, e:
        result = (str(e), 0)
    return result


def main():
    module = AnsibleModule(
        argument_spec=dict(
            login_user=dict(default=None),
            login_password=dict(default=None, no_log=True),
            login_host=dict(default="127.0.0.1"),
            login_port=dict(default="3306", type='int'),
            login_unix_socket=dict(default=None),
            query=dict(default=None),
            query_file=dict(default=None),
            db=dict(default="mysql"),
            ssl_cert=dict(default=None),
            ssl_key=dict(default=None),
            ssl_ca=dict(default=None),
            config_file=dict(default="~/.my.cnf")
        ),
        supports_check_mode=True
    )
    user = module.params["login_user"]
    password = module.params["login_password"]
    host = module.params["login_host"]
    port = module.params["login_port"]
    ssl_cert = module.params["ssl_cert"]
    ssl_key = module.params["ssl_key"]
    ssl_ca = module.params["ssl_ca"]
    config_file = module.params['config_file']
    config_file = os.path.expanduser(os.path.expandvars(config_file))
    db = module.params["db"]

    query = module.params["query"]
    query_file = module.params["query_file"]

    if query is None and query_file is None:
        module.fail_json(msg="Cannot run without query or query_file to operate with")
    if query is not None and query_file is not None:
        module.fail_json(msg="Cannon run with query and query_file both specified.")
    if not mysqldb_found:
        module.fail_json(msg="the python mysqldb module is required")
    else:
        warnings.filterwarnings('ignore', category=MySQLdb.Warning)

    try:
        cursor = mysql_connect(module, user, password, config_file, ssl_cert, ssl_key, ssl_ca, db)
    except Exception, e:
        if os.path.exists(config_file):
            module.fail_json(
                msg="unable to connect to database, check login_user and login_password "
                    "are correct or %s has the credentials. Exception message: %s" % (
                        config_file, e))
        else:
            module.fail_json(msg="unable to find %s. Exception message: %s" % (config_file, e))

    if query is not None:
        result = execute_query(cursor, query, module.check_mode)
        if result[0] is True:
            module.exit_json(msg="Query execution succeed", rows_affected=result[1], changed=(result[1] > 0))
    # if not query, we will execute a query file
    else:
        result = execute_query_file(cursor, query_file, module.check_mode)
        if result[0] is True:
            module.exit_json(msg="Query file execution succeed", rows_affected=result[1], changed=(result[1] > 0))

    module.fail_json(msg=result[0], changed=(result[1] > 0))


# import module snippets
from ansible.module_utils.basic import *
from ansible.module_utils.database import *
from ansible.module_utils.mysql import *

main()

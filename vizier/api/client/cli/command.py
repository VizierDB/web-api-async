# Copyright (C) 2017-2019 New York University,
#                         University at Buffalo,
#                         Illinois Institute of Technology.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Abstract class for interpreter commands. Each command has to implement two
methods:

- eval(list(string)): Given a list of tokens check whether the tokens reference
  the given command. If yes, evaluate the command and return True. Otherwise,
  return False.
- help: Print a simple help statement
"""

from abc import abstractmethod


class Command(object):
    """Abstract class for interpreter commands."""
    @abstractmethod
    def eval(self, tokens):
        """If the given tokens sequence matches the given command execute it
        and return True. Otherwise, return False.

        Parameters
        ----------
        tokens: list(string)
            List of tokens in the command line

        Returns
        -------
        bool
        """
        raise NotImplementedError

    @abstractmethod
    def help(self):
        """Print a simple help statement for the command."""
        raise NotImplementedError

    def output(self, rows):
        """Output the given rows in tabular format. Each rows is a list of
        string values. All rows are expected to have the sam elength. The first
        row is the table header.

        Parameters
        ----------
        rows: list(string)
            List of rows in the table
        """
        # Determine the longest value for each column.
        columns = [0] * len(rows[0])
        for row in rows:
            for col in range(len(columns)):
                l = len(row[col])
                if l > columns[col]:
                    columns[col] = l
        # Create format string
        format = None
        divider = list()
        for col_len in columns:
            f = '%-' + str(col_len) + 's'
            if format is None:
                format = f
            else:
                format += ' | ' + f
            if len(divider) in [0, len(columns) - 1]:
                i = 1
            else:
                i = 2
            divider.append('-' * (col_len + i))
        # Print fomrated rows
        print format % tuple(rows[0])
        print '|'.join(divider)
        for row in rows[1:]:
            print format % tuple(row)

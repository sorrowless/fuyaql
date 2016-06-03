#!/usr/bin/env python
"""Fuel YAQL real-time console.
Allow fast and easy test your YAQL expressions on live cluster.
Usage:
    fuyaql.py [-v...] [-n NODE] CLUSTER_ID
    fuyaql.py [-n NODE] -o OLD_CONTEXT -x EXPECTED_CONTEXT -e EXPRESSION
    fuyaql.py -h
    fuyaql.py --version

Options:
    -v                              verbosity level. Use more than one time to raise level
                                    (like -vvvv)
    -n --node NODE                  node which will used to compare contexts [default: master]
    -o --old OLD_CONTEXT            json file which will be used as a new context
    -x --expected EXPECTED_CONTEXT  json file which will be used as expected context
    -e --expression EXPRESSION      expression which should be evaluated
    -h --help                       show this help
    --version                       show version

Arguments:
    CLUSTER_ID  Cluster ID for which YAQL data will be gathered
"""

import completion
import json
import logging
import os
import readline
import sys
from docopt import docopt
from f_consts import reserved_commands
from nailgun import objects
from nailgun import yaql_ext
from nailgun.db import db
from nailgun.db.sqlalchemy.models import Cluster
from nailgun.orchestrator import deployment_serializers


class Options:
    def __init__(self):
        """ Initialize main variables.
        self.args     - store command line arguments
        self.options  - store options hash
        self.logger   - store logging instance
        """
        self.args = {}
        self.options = {}
        self.logger = False
        # Read command line options and set logging in constructor
        self._read_options()

    def _read_options(self):
        """ Reads command line options and saves it to self.args hash.
        """
        self.args = docopt(__doc__, version='0.5')
        self.options = self.args
        loglevel = (5 - self.args['-v'])*10 if self.args['-v'] < 5 else 10
        logging.basicConfig(
                level=loglevel,
                format='%(asctime)s %(name)-30s %(levelname)-9s %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Passed arguments is: %s', str(self.args))


class Fyaql:
    def __init__(self, options, logger):
        self.options = options
        self.logger = logger
        self.cluster_id = self.options['CLUSTER_ID']
        self.node_id = self.options['--node']

        self.cluster = None
        self.nodes_to_deploy = None

        self.expected_state = None

        self.old_context_task = None
        self.current_state = None
        self.context = None
        self.yaql_engine = None

    def get_cluster(self, cluster_id=None):
        if not cluster_id:
            cluster_id = self.cluster_id
        return db().query(Cluster).get(cluster_id)

    def get_nodes_to_deploy(self):
        self.nodes_to_deploy = list(
            objects.Cluster.get_nodes_not_for_deletion(self.cluster).all()
        )
        self.logger.debug('Nodes to deploy are: %s', self.nodes_to_deploy)

    def get_real_expected_state(self):
        expected_deployment_info = deployment_serializers.serialize_for_lcm(
            self.cluster,
            self.nodes_to_deploy
        )
        self.expected_state = {node['uid']: node for node in expected_deployment_info}
        self.logger.debug('Expected state is %s', self.expected_state)

    def get_last_successful_task(self):
        self.old_context_task = objects.TransactionCollection.get_last_succeed_run(
            self.cluster)

    def get_current_state(self):
        try:
            self.current_state = self.old_context_task.deployment_info
        except AttributeError:
            self.current_state = {}
        self.logger.debug('Current state is: %s', self.current_state)

    def get_contexts(self):
        main_yaql_context = yaql_ext.create_context(
            add_serializers=True, add_datadiff=True
        )
        self.context = main_yaql_context.create_child_context()

    def update_contexts(self):
        try:
            self.context['$%new'] = self.expected_state[self.node_id]
        except KeyError:
            self.context['$%new'] = self.expected_state['master']
        try:
            self.context['$%old'] = self.current_state[self.node_id]
        except KeyError:
            self.context['$%old'] = {}

    def create_evaluator(self):
        self.yaql_engine = yaql_ext.create_engine()

    def evaluate(self, yaql_expression):
        try:
            parsed_exp = self.yaql_engine(yaql_expression)
            self.logger.debug('parsed exp is: %s', parsed_exp)
            res = parsed_exp.evaluate(data=self.context['$%new'],
                                      context=self.context)
            self.logger.debug('Evaluation result is: %s', res)
        except:
            self.logger.debug('Exception caught')
            res = '<Cannot evaluate this, exception caught>'
        return res

    def create_structure(self):
        self.cluster = self.get_cluster()
        self.logger.debug('Cluster instance is: %s', self.cluster)
        if not self.cluster:
            return
        self.get_nodes_to_deploy()
        self.get_real_expected_state()
        self.get_last_successful_task()
        self.get_current_state()
        self.get_contexts()
        self.update_contexts()
        self.create_evaluator()

    def parse_command(self, command):
        if command.startswith(':show'):
            return command, None
        data = command.split(' ')
        value = data.pop()
        command = ' '.join(data)
        return command, value

    def show_cluster(self):
        print('Cluster id is: %s, name is: %s' %
              (self.cluster.id, self.cluster.name))

    def show_tasks(self):
        tasks = [task.id for task in self.cluster.tasks if task.deployment_info]
        print('This cluster has next ids which you can use as context ' +
              'sources: %s' % tasks)
        if self.old_context_task.id in tasks:
            print('Currently task with id %s is used as old context source' %
                  self.old_context_task.id)

    def use_old_context_from_task(self, task_id):
        tasks = [task for task in self.cluster.tasks if task.deployment_info]
        task = [task for task in tasks if str(task.id) == task_id]
        if not task:
            print("There is no task with id %s, can't switch to it" % task_id)
            return
        self.old_context_task = task[0]
        self.get_current_state()
        self.update_contexts()

    def use_new_context_from_task(self, task_id):
        tasks = [task for task in self.cluster.tasks if task.deployment_info]
        task = [task for task in tasks if str(task.id) == task_id]
        if not task:
            print("There is no task with id %s, can't switch to it" % task_id)
            return
        self.expected_state = task[0].deployment_info
        self.update_contexts()

    def show_nodes(self):
        nodes_ids = {}
        for node in self.cluster.nodes:
            nodes_ids[node.id] = str(', '.join(node.all_roles))
        print('Cluster has nodes with ids: %s' % nodes_ids)

    def show_node(self):
        print('Currently used node id is: %s' % self.node_id)

    def use_cluster(self, cluster_id):
        cluster = self.get_cluster(cluster_id)
        if not cluster:
            print("There is no cluster with id %s, can't switch to it" %
                  cluster_id)
            return
        self.cluster_id = cluster_id
        self.logger.info("Cluster changed, reset default node id to master")
        self.node_id = 'master'
        self.create_structure()

    def use_node(self, node_id):
        if node_id not in [str(node.id) for node in self.cluster.nodes]:
            print('There is no node with id %s in cluster %s' %
                  (node_id, self.cluster_id))
        self.node_id = node_id
        self.update_contexts()

    def run_internal_command(self, command, value):
        if command not in reserved_commands:
            print('Unknown internal command')
            return
        if value:
            getattr(self, reserved_commands[command])(value)
        else:
            getattr(self, reserved_commands[command])()

    def get_console(self):
        command = True

        while command != 'exit':
            try:
                command = raw_input('fuel-yaql> ')
            except EOFError:
                return
            if not command:
                continue

            if command.startswith(':'):
                command, value = self.parse_command(command)
                self.run_internal_command(command, value)
            else:
                result = self.evaluate(command)
                print(json.dumps(result, indent=4))


def lean_contexts(opts):
    evaluator = Fyaql(opts.options, opts.logger)
    try:
        current_path = evaluator.options['--old']
        with open(os.path.expanduser(current_path), 'r') as f:
            current_state = json.load(f)
        expected_path = evaluator.options['--expected']
        with open(os.path.expanduser(expected_path), 'r') as f:
            expected_state = json.load(f)
    except IOError:
        sys.exit(1)
    evaluator.get_contexts()
    evaluator.context['$%new'] = expected_state
    evaluator.context['$%old'] = current_state
    evaluator.create_evaluator()

    expression = evaluator.options['--expression']
    try:
        parsed_exp = evaluator.yaql_engine(expression)
        res = parsed_exp.evaluate(data=evaluator.context['$%new'],
                                  context=evaluator.context)
        result = 0
    except:
        result = 1
    sys.exit(result)


def main():
    readline.set_completer_delims(r'''`~!@#$%^&*()-=+[{]}\|;'",<>/?''')
    readline.set_completer(completion.FuCompleter(
        reserved_commands.keys()
    ).complete)
    readline.parse_and_bind('tab: complete')
    opts = Options()
    # If there is passed contexts - just compare them and exit
    if opts.options['--old']:
        lean_contexts(opts)
    interpret = Fyaql(opts.options, opts.logger)
    interpret.create_structure()
    interpret.get_console()

if __name__ == '__main__':
    main()

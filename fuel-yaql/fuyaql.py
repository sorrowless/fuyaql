#!/usr/bin/env python
"""Fuel YAQL real-time console.
Allow fast and easy test your YAQL expressions on live cluster.
Usage:
    fuyaql.py [-v...] CLUSTER_ID
    fuyaql.py -h
    fuyaql.py --version

Options:
    -v         verbosity level. Use more than one time to raise level (like -vvvv)
    -h --help  show this help
    --version  show version

Arguments:
    CLUSTER_ID      Cluster ID for which YAQL data will be gathered
"""

import json
import logging
from docopt import docopt
from nailgun import consts
from nailgun import objects
from nailgun import yaql_ext
from nailgun.db import db
from nailgun.db.sqlalchemy.models import Cluster
from nailgun.db.sqlalchemy.models import Task
from nailgun.orchestrator import deployment_serializers
from nailgun.task.task import ClusterTransaction

reserved_commands = {
    ':show cluster': 'show_cluster',
    ':show nodes': 'show_nodes',
    ':show node': 'show_node',
    ':use cluster': 'use_cluster',
    ':use node': 'use_node',
    ':load lastrun': 'update_old_context',
}


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
        self.args = docopt(__doc__, version='0.2')
        self.options = self.args
        loglevel = (5 - self.args['-v'])*10 if self.args['-v'] < 5 else 10
        logging.basicConfig(
                level=loglevel,
                format='%(asctime)s %(name)-30s %(levelname)-9s %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Passed arguments is: %s', str(self.args))


class Fyaql:
    def __init__(self, options):
        self.options = options.options
        self.logger = options.logger
        self.cluster_id = self.options['CLUSTER_ID']
        self.node_id = 'master'
        self.cluster = None
        self.nodes_to_deploy = None
        self.supertask = None
        self.deployment = None
        self.deployment_info = None
        self.expected_state = None
        self.deployment_tasks = None
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

    def get_supertask(self):
        self.supertask = Task(
            name=consts.TASK_NAMES.deploy, cluster=self.cluster,
            status=consts.TASK_STATUSES.pending)
        self.logger.debug('Supertask instance is: %s', self.supertask)

    def get_deployment_cycle(self):
        self.deployment = self.supertask.create_subtask(
                name=consts.TASK_NAMES.deployment,
                status=consts.TASK_STATUSES.pending
            )
        self.logger.debug('Deployment task instance is: %s', self.deployment)

    def get_deployment_info(self):
        self.deployment_info = deployment_serializers.serialize_for_lcm(
            self.deployment.cluster,
            self.nodes_to_deploy
        )
        self.logger.debug('Deployment info is: %s', self.deployment_info)

    def get_expected_state(self):
        self.expected_state = ClusterTransaction._save_deployment_info(
            self.deployment,
            self.deployment_info
        )
        self.logger.debug('Expected state is %s', self.expected_state)

    def get_tasks(self):
        self.deployment_tasks = objects.Cluster.get_deployment_tasks(
            self.deployment.cluster,
            None
        )
        self.logger.debug('Deployment tasks are %s', self.deployment_tasks)

    def get_current_state(self):
        last_run = objects.TransactionCollection.get_last_succeed_run(
            self.cluster)
        try:
            self.current_state = last_run.deployment_info
        except AttributeError:
            self.current_state = {}
        self.logger.debug('Current state is: %s', self.current_state)

    def get_contexts(self):
        main_yaql_context = yaql_ext.create_context(
            add_serializers=True, add_datadiff=True
        )
        self.context = main_yaql_context.create_child_context()
        self.update_contexts()

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
        self.get_supertask()
        self.get_deployment_cycle()
        self.get_deployment_info()
        self.get_expected_state()
        self.get_tasks()
        self.get_current_state()
        self.get_contexts()
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

    def load_lastrun(self):
        pass

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
                print json.dumps(result, indent=4)


if __name__ == '__main__':
    opts = Options()
    interpret = Fyaql(opts)
    interpret.create_structure()
    interpret.get_console()


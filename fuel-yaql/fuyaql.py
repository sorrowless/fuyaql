#!/usr/bin/env python
"""Fuel YAQL real-time console.
Allow fast and easy test your YAQL expressions on live cluster.
Usage:
    fuyaql.py CLUSTER_ID
    fuyaql.py -h
    fuyaql.py --version
Options:
    -h --help       show this help
    --version       show version

Arguments:
    CLUSTER_ID      Cluster ID for which YAQL data will be gathered
"""

import logging
from docopt import docopt
from nailgun import consts
from nailgun import lcm
from nailgun import objects
from nailgun import yaql_ext
from nailgun.db import db
from nailgun.db.sqlalchemy.models import Cluster
from nailgun.db.sqlalchemy.models import Task
from nailgun.orchestrator import deployment_serializers
from nailgun.task.task import ClusterTransaction


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
        logging.basicConfig(
                level=logging.DEBUG,
                format='%(asctime)s %(name)-30s %(levelname)-9s %(message)s')
        self.logger = logging.getLogger(__name__)
        self.logger.debug('Passed arguments is: %s', str(self.args))


class Fyaql:
    def __init__(self, options):
        self.options = options.options
        self.logger = options.logger
        self.cluster_id = self.options['CLUSTER_ID']
        self.cluster = self.get_cluster()
        self.nodes_to_deploy = self.get_nodes_to_deploy()

    def get_cluster(self):
        return db().query(Cluster).get(self.cluster_id)

    def get_nodes_to_deploy(self):
        return list(
            objects.Cluster.get_nodes_not_for_deletion(self.cluster).all()
        )

    def run(self):
        self.logger.debug('Cluster instance is: %s', self.cluster)
        if not self.cluster:
            return
        supertask = Task(name=consts.TASK_NAMES.deploy, cluster=self.cluster,
                         status=consts.TASK_STATUSES.pending)
        self.logger.debug('Supertask instance is: %s', supertask)
        task_deployment = supertask.create_subtask(
                name=consts.TASK_NAMES.deployment,
                status=consts.TASK_STATUSES.pending
            )
        self.logger.debug('Deployment task instance is: %s', task_deployment)
        self.logger.debug('Nodes to deploy are: %s', self.nodes_to_deploy)
        deployment_info = deployment_serializers.serialize_for_lcm(
            task_deployment.cluster,
            self.nodes_to_deploy
        )
        self.logger.debug('Deployment info is: %s', deployment_info)
        expected_state = ClusterTransaction._save_deployment_info(
            task_deployment,
            deployment_info
        )
        self.logger.debug('Expected state is %s', expected_state)
        deployment_tasks = objects.Cluster.get_deployment_tasks(
            task_deployment.cluster,
            None
        )
        self.logger.debug('Deployment tasks are %s', deployment_tasks)


        ignored_types = {
            consts.ORCHESTRATOR_TASK_TYPES.skipped,
            consts.ORCHESTRATOR_TASK_TYPES.group,
            consts.ORCHESTRATOR_TASK_TYPES.stage,
        }

        tasks_names = [t['id'] for t in deployment_tasks
                       if t['type'] not in ignored_types]
        transaction_collection = objects.TransactionCollection
        transactions = (
            transaction_collection.get_successful_transactions_per_task(
                task_deployment.cluster.id, tasks_names)
        )
        current_state = {
            task_id: objects.Transaction.get_deployment_info(tr)
            for tr, task_id in transactions
        }
        self.logger.debug('Current state is: %s', current_state)

        _transaction_context = lcm.TransactionContext(expected_state, current_state)

        self._yaql_context = yaql_ext.create_context(
            add_serializers=True, add_datadiff=True
        )
        self._yaql_engine = yaql_ext.create_engine()
        child_context = self._yaql_context.create_child_context()
        try:
            child_context['$%new'] = _transaction_context.get_new_data('1')
        except KeyError:
            child_context['$%new'] = _transaction_context.get_new_data('master')
        try:
            child_context['$%old'] = _transaction_context.get_old_data('1', '1')
        except KeyError:
            child_context['$%old'] = {}

        parsed_exp = self._yaql_engine('changed($)')
        self.logger.debug('parsed exp is: %s', parsed_exp)
        res = parsed_exp.evaluate(data=child_context['$%new'], context=child_context)
        self.logger.debug('Evaluation result is: %s', res)


if __name__ == '__main__':
    opts = Options()
    interpret = Fyaql(opts)
    interpret.run()

# Fuel-YAQL

Fuel-YAQL is live YAQL master node console for Fuel to easy evaluate yaql
conditions user wanted to put into task.

How to use:

checkout this repo on Fuel master node, run

> python setup.py install

> fuelyaql 'CLUSTER_ID'

where 'CLUSTER_ID' is id of existing cluster which you can get by run

> fuel env

command. Cluster id there needed to have opportunity for use internal Fuel yaql
functions like 'changed' or 'new'. After this fuel-yaql console will be opened:

> fuel-yaql >

there you can evaluate all functions and conditions you need just by entering
them, for example:

> fuel-yaql> changed($)

> true

This console has some internal commands. There they are:

> fuel-yaql> :show cluster

> Cluster id is: 1, name is: test

shows you cluster you currently use.

> fuel-yaql> :show node

> Currently used node id is: master

shows you node in this cluster for which conditions will be evaluated.

> fuel-yaql> :show nodes

> Cluster has nodes with ids: {1: 'controller'}

shows you all nodes in this cluster

> fuel-yaql> :use cluster 1

will switch contexts to another cluster

> fuel-yaql> :use node 1

will switch contexts to another node


Fuel itself has several internal yaql functions which is not included to base
yaql interpreter. There they are:

```changed()``` - will show you the difference between new and old contexts

```new()``` - returns you new context data

```old()``` - returns you old context data

```added()``` - returns you diff what was added between old and new context

```deleted()``` - returns you diff what was deleted between old and new contexts

```changedAll($.first, $.second, $.Nth)``` - returns True if all expressions in
parentheses returns non-False

```changedAny($.first, $.second, $.Nth)``` - returns True if any expression in
parentheses returns non-False

I believe, it is mostly all you need to debug yaql expressions on fuel-node. For
any questions - don't hesitate ask me.

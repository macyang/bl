MyApp
=============================

This is an example BlueLake DataHub application.

To deploy the application,
% cd myapp-ear
% mvn appengine:update

To test the application,
% curl -X POST -d @example/job.json http://<appname>.appspot.com/datahub/v1beta1/jobs

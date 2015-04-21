angular.module(PKG.name + '.feature.etlapps')
  .controller('EtlAppsListController', function($scope, MyDataSource, mySettings) {
    var dataSrc = new MyDataSource($scope);
    $scope.etlapps  = [];
    dataSrc.request({
      _cdapNsPath: '/adapters?template=etl.batch'
    })
      .then(function(res) {
        if (!res.length) {
          return;
        }
        $scope.etlapps = res;
        angular.forEach($scope.etlapps, function(app) {
          app.status =  (Date.now()/2)? 'Running': 'Stopped';
          app.description = 'Something something dark.Something Something something dark';
        });
      });
    mySettings.get('etldrafts')
      .then(function(res) {
        if (Object.keys(res).length) {
          angular.forEach(res, function(value, key) {
            $scope.etlapps.push({
              isdraft: true,
              name: key,
              template: value.config.metadata.type,
              status: (Date.now()/2)? 'Running': 'Stopped',
              description: 'Something something dark.Something Something something dark'
            });
          });
        }
      });
  });

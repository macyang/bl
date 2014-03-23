'use strict';

/* Controllers */

var phonecatApp = angular.module('phonecatApp', []);

phonecatApp.controller('PhoneListCtrl', function($scope, $http) {
console.log($scope.limit);
  $http({
      method: 'GET',
      url: 'http://lotus-stresstest.appspot.com/guide/passport?limit=' + $scope.limit
    }
  ).success(
    function(data) {
      $scope.phones = data;
    }
  );

  $scope.orderProp = '-checkinTimestamp';
});

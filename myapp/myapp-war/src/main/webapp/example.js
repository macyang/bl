var plunker = angular.module('plunker', ['ui.bootstrap']);

plunker.controller('AccordionDemoCtrl', function($scope) {
  $scope.oneAtATime = true;

  $scope.groups = [
    {
      title: "Dynamic Group Header - 1",
      content: "Dynamic Group Body - 1"
    },
    {
      title: "Dynamic Group Header - 2",
      content: "Dynamic Group Body - 2"
    }
  ];

  $scope.items = ['Item 1', 'Item 2', 'Item 3'];

  $scope.addItem = function() {
    var newItemNo = $scope.items.length + 1;
    $scope.items.push('Item ' + newItemNo);
  };
});

plunker.controller('PhoneListCtrl', function($scope, $http) {
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

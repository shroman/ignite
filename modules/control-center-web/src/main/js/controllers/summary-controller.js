/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Controller for Summary screen.
consoleModule.controller('summaryController', [
    '$scope', '$http', '$common', '$loading', '$message',
    function ($scope, $http, $common, $loading, $message) {
    $scope.joinTip = $common.joinTip;
    $scope.getModel = $common.getModel;

    $scope.showMoreInfo = $message.message;

    $scope.javaClassItems = [
        {label: 'snippet', value: 1},
        {label: 'factory class', value: 2}
    ];

    $scope.evictionPolicies = [
        {value: 'LRU', label: 'LRU'},
        {value: 'RND', label: 'Random'},
        {value: 'FIFO', label: 'FIFO'},
        {value: 'SORTED', label: 'Sorted'},
        {value: undefined, label: 'Not set'}
    ];

    $scope.tabs = { activeTab: 0 };

    $scope.pojoClasses = function() {
        var classes = [];

        _.forEach($generatorJava.metadatas, function(meta) {
            classes.push(meta.keyType);
            classes.push(meta.valueType);
        });

        return classes;
    };

    $scope.oss = ['debian:8', 'ubuntu:14.10'];

    $scope.configServer = {javaClassServer: 1, os: undefined};

    $scope.backupItem = {javaClassClient: 1};

    $http.get('/models/summary.json')
        .success(function (data) {
            $scope.screenTip = data.screenTip;
            $scope.moreInfo = data.moreInfo;
            $scope.clientFields = data.clientFields;
        })
        .error(function (errMsg) {
            $common.showError(errMsg);
        });

    $scope.clusters = [];

    $scope.aceInit = function (editor) {
        editor.setReadOnly(true);
        editor.setOption('highlightActiveLine', false);
        editor.setAutoScrollEditorIntoView(true);
        editor.$blockScrolling = Infinity;

        var renderer = editor.renderer;

        renderer.setHighlightGutterLine(false);
        renderer.setShowPrintMargin(false);
        renderer.setOption('fontSize', '14px');
        renderer.setOption('minLines', '3');
        renderer.setOption('maxLines', '50');

        editor.setTheme('ace/theme/chrome');
    };

    $scope.generateJavaServer = function () {
        $scope.javaServer = $generatorJava.cluster($scope.selectedItem, $scope.configServer.javaClassServer === 2);
    };

    function selectPojoClass() {
        _.forEach($generatorJava.metadatas, function(meta) {
            if (meta.keyType == $scope.configServer.pojoClass)
                $scope.pojoClass = meta.keyClass;
            else if (meta.valueType == $scope.configServer.pojoClass)
                $scope.pojoClass = meta.valueClass;
        });
    }

    $scope.updatePojos = function() {
        if ($common.isDefined($scope.selectedItem)) {
            var curCls = $scope.configServer.pojoClass;

            $generatorJava.pojos($scope.selectedItem.caches, $scope.configServer.useConstructor, $scope.configServer.includeKeyFields);

            if (!$common.isDefined(curCls) || _.findIndex($generatorJava.metadatas, function (meta) {
                    return meta.keyType == curCls || meta.valueType == curCls;
                }) < 0) {
                if ($generatorJava.metadatas.length > 0) {
                    if ($common.isDefined($generatorJava.metadatas[0].keyType))
                        $scope.configServer.pojoClass = $generatorJava.metadatas[0].keyType;
                    else
                        $scope.configServer.pojoClass = $generatorJava.metadatas[0].valueType;
                }
                else {
                    $scope.configServer.pojoClass = undefined;

                    if ($scope.tabs.activeTab == 2)
                        $scope.tabs.activeTab = 0;
                }
            }
            else
                $scope.configServer.pojoClass = curCls;

            selectPojoClass();
        }
    };

    $scope.$watch('configServer.javaClassServer', $scope.generateJavaServer, true);

    $scope.$watch('configServer.pojoClass', selectPojoClass, true);

    $scope.$watch('configServer.useConstructor', $scope.updatePojos, true);

    $scope.$watch('configServer.includeKeyFields', $scope.updatePojos, true);

    $scope.generateDockerServer = function() {
        var os = $scope.configServer.os ? $scope.configServer.os : $scope.oss[0];

        $scope.dockerServer = $generatorDocker.clusterDocker($scope.selectedItem, os);
    };

    $scope.$watch('configServer.os', $scope.generateDockerServer, true);

    $scope.generateClient = function () {
        $scope.xmlClient = $generatorXml.cluster($scope.selectedItem, $scope.backupItem.nearConfiguration);
        $scope.javaClient = $generatorJava.cluster($scope.selectedItem, $scope.backupItem.javaClassClient === 2,
            $scope.backupItem.nearConfiguration, $scope.configServer.useConstructor);
    };

    $scope.$watch('backupItem', $scope.generateClient, true);

    $scope.selectItem = function (cluster) {
        if (!cluster)
            return;

        $scope.selectedItem = cluster;

        $scope.xmlServer = $generatorXml.cluster(cluster);

        $scope.generateJavaServer();

        $scope.generateDockerServer();

        $scope.generateClient();

        $scope.updatePojos();
    };

    $scope.pojoAvailable = function() {
        return $common.isDefined($generatorJava.metadatas) && $generatorJava.metadatas.length > 0;
    };

    $loading.start('loadingSummaryScreen');

    $http.post('clusters/list')
        .success(function (data) {
            $scope.clusters = data.clusters;

            if ($scope.clusters.length > 0) {
                // Populate clusters with caches.
                _.forEach($scope.clusters, function (cluster) {
                    cluster.caches = _.filter(data.caches, function (cache) {
                        return _.contains(cluster.caches, cache._id);
                    });
                });

                var restoredId = sessionStorage.summarySelectedId;

                var selectIdx = 0;

                if (restoredId) {
                    var idx = _.findIndex($scope.clusters, function (cluster) {
                        return cluster._id == restoredId;
                    });

                    if (idx >= 0)
                        selectIdx = idx;
                    else
                        delete sessionStorage.summarySelectedId;
                }

                $scope.selectItem($scope.clusters[selectIdx]);

                $scope.$watch('selectedItem', function (val) {
                    if (val)
                        sessionStorage.summarySelectedId = val._id;
                }, true);
            }
        })
        .finally(function () {
            $loading.finish('loadingSummaryScreen');
        });
}]);

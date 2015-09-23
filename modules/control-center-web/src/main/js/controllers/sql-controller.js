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

// Controller for SQL notebook screen.
controlCenterModule.controller('sqlController',
    ['$scope', '$window','$controller', '$http', '$timeout', '$common', '$confirm', '$interval', '$popover', '$loading',
    function ($scope, $window, $controller, $http, $timeout, $common, $confirm, $interval, $popover, $loading) {
    // Initialize the super class and extend it.
    angular.extend(this, $controller('agent-download', {$scope: $scope}));
    $scope.agentGoal = 'execute sql statements';
    $scope.agentTestDriveOption = '--test-drive-sql';

    $scope.joinTip = $common.joinTip;

    $scope.caches = [];

    $scope.pageSizes = [50, 100, 200, 400, 800, 1000];

    $scope.modes = $common.mkOptions(['PARTITIONED', 'REPLICATED', 'LOCAL']);

    $scope.timeUnit = [
        {value: 1000, label: 'seconds', short: 's'},
        {value: 60000, label: 'minutes', short: 'm'},
        {value: 3600000, label: 'hours', short: 'h'}
    ];

    $scope.exportDropdown = [{ 'text': 'Export all', 'click': 'exportAll(paragraph)'}];

    $scope.floatTheadOptions = {
        autoReflow: true,
        useAbsolutePositioning: true,
        scrollContainer: function($table) {
            return $table.closest(".sql-table-wrapper");
        }
    };

    $scope.treeOptions = {
        nodeChildren: "children",
        dirSelectable: true,
        injectClasses: {
            iExpanded: "fa fa-minus-square-o",
            iCollapsed: "fa fa-plus-square-o"
        }
    };

    var _hideColumn = function (col) {
        return !(col.fieldName === "_KEY") && !(col.fieldName == "_VAL");
    };

    var _allColumn = function () {
        return true;
    };

    var paragraphId = 0;

    function enhanceParagraph(paragraph) {
        paragraph.chart = function () {
            return this.result != 'table' && this.result != 'none';
        };

        paragraph.nonEmpty = function () {
            return this.rows && this.rows.length > 0;
        };
    }

    $scope.aceInit = function (paragraph) {
        return function (editor) {
            editor.setAutoScrollEditorIntoView(true);
            editor.$blockScrolling = Infinity;

            var renderer = editor.renderer;

            renderer.setHighlightGutterLine(false);
            renderer.setShowPrintMargin(false);
            renderer.setOption('fontSize', '14px');
            renderer.setOption('minLines', '5');
            renderer.setOption('maxLines', '15');

            editor.setTheme('ace/theme/chrome');

            Object.defineProperty(paragraph, 'ace', { value: editor });
        }
    };

    var loadNotebook = function () {
        $http.post('/notebooks/get', {noteId: $scope.noteId})
            .success(function (notebook) {
                $scope.notebook = notebook;

                $scope.notebook_name = notebook.name;

                _.forEach(notebook.paragraphs, function (paragraph) {
                    paragraph.id = paragraphId++;

                    enhanceParagraph(paragraph);
                });

                if (!notebook.paragraphs || notebook.paragraphs.length == 0)
                    $scope.addParagraph();
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    loadNotebook();

    var _saveNotebook = function (f) {
        $http.post('/notebooks/save', $scope.notebook)
            .success(f || function() {})
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.renameNotebook = function (name) {
        if (!name)
            return;

        if ($scope.notebook.name != name) {
            $scope.notebook.name = name;

            _saveNotebook(function () {
                var idx = _.findIndex($scope.$root.notebooks, function (item) {
                    return item._id == $scope.notebook._id;
                });

                if (idx >= 0) {
                    $scope.$root.notebooks[idx].name = name;

                    $scope.$root.rebuildDropdown();
                }

                $scope.notebook.edit = false;
            });
        }
        else
            $scope.notebook.edit = false
    };

    $scope.removeNotebook = function () {
        $confirm.confirm('Are you sure you want to remove notebook: "' + $scope.notebook.name + '"?')
            .then(function () {
                $http.post('/notebooks/remove', {_id: $scope.notebook._id})
                    .success(function () {
                        var idx = _.findIndex($scope.$root.notebooks, function (item) {
                            return item._id == $scope.notebook._id;
                        });

                        if (idx >= 0) {
                            $scope.$root.notebooks.splice(idx, 1);

                            if ($scope.$root.notebooks.length > 0)
                                $window.location = "/sql/" +
                                    $scope.$root.notebooks[Math.min(idx,  $scope.$root.notebooks.length - 1)]._id;
                            else
                                $window.location = '/configuration/clusters';
                        }
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            });
    };

    $scope.renameParagraph = function (paragraph, newName) {
        if (!newName)
            return;

        if (paragraph.name != newName) {
            paragraph.name = newName;

            _saveNotebook(function () { paragraph.edit = false; });
        }
        else
            paragraph.edit = false
    };

    $scope.addParagraph = function () {
        if (!$scope.notebook.paragraphs)
            $scope.notebook.paragraphs = [];

        var sz = $scope.notebook.paragraphs.length;

        var paragraph = {
            id: paragraphId++,
            name: 'Query' + (sz ==0 ? '' : sz),
            editor: true,
            query: '',
            pageSize: $scope.pageSizes[0],
            result: 'none',
            rate: {
                value: 1,
                unit: 60000,
                installed: false
            }
        };

        enhanceParagraph(paragraph);

        if ($scope.caches && $scope.caches.length > 0)
            paragraph.cache = $scope.caches[0];

        $scope.notebook.expandedParagraphs.push($scope.notebook.paragraphs.length);

        $scope.notebook.paragraphs.push(paragraph);
    };

    $scope.setResult = function (paragraph, new_result) {
        var changed = paragraph.result != new_result;

        paragraph.result = paragraph.result === new_result ? 'none' : new_result;

        if (changed && paragraph.chart())
            $scope.applyChartSettings(paragraph);
    };

    $scope.resultEq = function(paragraph, result) {
        return (paragraph.result === result);
    };

    $scope.removeParagraph = function(paragraph) {
        $confirm.confirm('Are you sure you want to remove paragraph: "' + paragraph.name + '"?')
            .then(function () {
                    var paragraph_idx = _.findIndex($scope.notebook.paragraphs, function (item) {
                        return paragraph == item;
                    });

                    var panel_idx = _.findIndex($scope.notebook.expandedParagraphs, function (item) {
                        return paragraph_idx == item;
                    });

                    if (panel_idx >= 0)
                        $scope.notebook.expandedParagraphs.splice(panel_idx, 1);

                    $scope.notebook.paragraphs.splice(paragraph_idx, 1);
            });
    };

    $http.post('/agent/topology')
        .success(function (caches) {
            $scope.caches = _.sortBy(caches, 'name');

            if ($scope.caches.length > 0)
                _.forEach($scope.notebook.paragraphs, function (paragraph) {
                    if (!paragraph.cacheName || !_.find($scope.caches, {name: paragraph.cacheName}))
                        paragraph.cacheName = $scope.caches[0].name;
                });
        })
        .error(function (err, status) {
            if (status == 503)
                $scope.showDownloadAgent();
            else
                $common.showError('Receive agent error: ' + err);
        });


    var _columnFilter = function(paragraph) {
        return paragraph.disabledSystemColumns || paragraph.systemColumns ? _allColumn : _hideColumn;
    };

    $scope.toggleSystemColumns = function (paragraph) {
        if (paragraph.disabledSystemColumns)
            return;

        paragraph.systemColumns = !paragraph.systemColumns;

        paragraph.columnFilter = _columnFilter(paragraph);
    };

    var _selectAxis = function (cols, col, dfltIdx) {
        if (col) {
            var idx = _.findIndex(cols, function (item) {
                return item.label == col.label;
            });

            if (idx >= 0)
                return cols[idx];
        }

        return cols.length >= dfltIdx ? cols[dfltIdx] : null;
    };

    var _processQueryResult = function (paragraph) {
        return function (res) {
            paragraph.meta = [];
            paragraph.chartColumns = [];

            if (res.meta) {
                paragraph.disabledSystemColumns = res.meta.length == 2 &&
                    res.meta[0].fieldName === "_KEY" && res.meta[1].fieldName === "_VAL";

                paragraph.columnFilter = _columnFilter(paragraph);

                paragraph.meta = res.meta;

                var idx = 0;

                _.forEach(res.meta, function (meta) {
                    var col = {value: idx++, label: meta.fieldName};

                    if (paragraph.disabledSystemColumns || _hideColumn(meta))
                        paragraph.chartColumns.push(col);
                });

                paragraph.chartColX = _selectAxis(paragraph.chartColumns, paragraph.chartColX, 0);
                paragraph.chartColY = _selectAxis(paragraph.chartColumns, paragraph.chartColY, 1);
            }

            paragraph.page = 1;

            paragraph.total = 0;

            paragraph.queryId = res.queryId;

            delete paragraph.errMsg;

            paragraph.rows = res.rows;

            if (paragraph.result == 'none')
                paragraph.result = 'table';
            else if (paragraph.chart())
                $scope.applyChartSettings(paragraph);
        }
    };

    var _executeRefresh = function (paragraph) {
        // TODO IGNITE-843 Temporary disable charts refresh by timer
        if (paragraph.rate.installed && paragraph.chart())
            return;

        $http.post('/agent/query', paragraph.queryArgs)
            .success(_processQueryResult(paragraph))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;
            });
    };

    $scope.execute = function (paragraph) {
        _saveNotebook();

        paragraph.queryArgs = { query: paragraph.query, pageSize: paragraph.pageSize, cacheName: paragraph.cacheName };

        $http.post('/agent/query', paragraph.queryArgs)
            .success(function (res) {
                _processQueryResult(paragraph)(res);

                _tryStartRefresh(paragraph);
            })
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;

                $scope.stopRefresh(paragraph);
            });
    };

    $scope.explain = function (paragraph) {
        _saveNotebook();

        _cancelRefresh(paragraph);

        $http.post('/agent/query', {query: 'EXPLAIN ' + paragraph.query, pageSize: paragraph.pageSize, cacheName: paragraph.cacheName})
            .success(_processQueryResult(paragraph))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;
            });
    };

    $scope.scan = function (paragraph) {
        _saveNotebook();

        _cancelRefresh(paragraph);

        $http.post('/agent/scan', {pageSize: paragraph.pageSize, cacheName: paragraph.cacheName})
            .success(_processQueryResult(paragraph))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;
            });
    };

    $scope.nextPage = function(item) {
        $http.post('/agent/query/fetch', {queryId: item.queryId, pageSize: item.pageSize, cacheName: item.cacheName})
            .success(function (res) {
                item.page++;

                item.total += item.rows.length;

                item.rows = res.rows;

                if (res.last)
                    delete item.queryId;
            })
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;
            });
    };

    var _export = function(fileName, meta, rows) {
        var csvContent = "";

        if (meta) {
            csvContent += meta.map(function (col) {
                return $scope.columnToolTip(col);
            }).join(",") + '\n';
        }

        rows.forEach(function (row) {
            if (Array.isArray(row)) {
                csvContent += row.map(function (elem) {
                    return elem ? JSON.stringify(elem) : "";
                }).join(",");
            }
            else {
                var first = true;

                for (var prop of meta) {
                    if (first)
                        first = false;
                    else
                        csvContent += ",";

                    var elem = row[prop.fieldName];

                    csvContent += elem ? JSON.stringify(elem) : "";
                }
            }

            csvContent += '\n';
        });

        $common.download('application/octet-stream;charset=utf-8', fileName, escape(csvContent));
    };

    $scope.exportPage = function(paragraph) {
        _export(paragraph.name + '.csv', paragraph.meta, paragraph.rows);
    };

    $scope.exportAll = function(paragraph) {
        $http.post('/agent/query/getAll', {query: paragraph.query, cacheName: paragraph.cacheName})
            .success(function (item) {
                _export(paragraph.name + '-all.csv', item.meta, item.rows);
            })
            .error(function (errMsg) {
                $common.showError(errMsg);
            });
    };

    $scope.columnToolTip = function (col) {
        var res = [];

        if (col.schemaName)
            res.push(col.schemaName);
        if (col.typeName)
            res.push(col.typeName);

        res.push(col.fieldName);

        return res.join(".");
    };

    $scope.resultMode = function (paragraph, type) {
        return (paragraph.result === type);
    };

    $scope.rateAsString = function (paragraph) {
        if (paragraph.rate && paragraph.rate.installed) {
            var idx = _.findIndex($scope.timeUnit, function (unit) {
                return unit.value == paragraph.rate.unit;
            });

            if (idx >= 0)
                return " " + paragraph.rate.value + $scope.timeUnit[idx].short;

            paragraph.rate.installed = false;
        }

        return "";
    };

    var _cancelRefresh = function (paragraph) {
        if (paragraph.rate && paragraph.rate.stopTime) {
            delete paragraph.queryArgs;

            paragraph.rate.installed = false;

            $interval.cancel(paragraph.rate.stopTime);

            delete paragraph.rate.stopTime;
        }
    };

    var _tryStopRefresh = function (paragraph) {
        if (paragraph.rate && paragraph.rate.stopTime) {
            $interval.cancel(paragraph.rate.stopTime);

            delete paragraph.rate.stopTime;
        }
    };

    var _tryStartRefresh = function (paragraph) {
        _tryStopRefresh(paragraph);

        if (paragraph.rate && paragraph.rate.installed && paragraph.queryArgs) {
            _executeRefresh(paragraph);

            var delay = paragraph.rate.value * paragraph.rate.unit;

            paragraph.rate.stopTime = $interval(_executeRefresh, delay, 0, false, paragraph);
        }
    };

    $scope.startRefresh = function (paragraph, value, unit) {
        paragraph.rate.value = value;
        paragraph.rate.unit = unit;
        paragraph.rate.installed = true;

        _tryStartRefresh(paragraph);
    };

    $scope.stopRefresh = function (paragraph) {
        paragraph.rate.installed = false;

        _tryStopRefresh(paragraph);
    };

    $scope.getter = function (value) {
        return value;
    };

    function _chartNumber(arr, idx, dflt) {
        if (arr && arr.length > idx) {
            var val = arr[idx];

            if (_.isNumber(val))
                return val;
        }

        return dflt;
    }

    function _chartLabel(arr, idx, dflt) {
        if (arr && arr.length > idx)
            return arr[idx];

        return dflt;
    }

    function _chartDatum(key, paragraph) {
        var index = 0;

        var values = _.map(paragraph.rows, function (row) {
            return {
                x: _chartNumber(row, paragraph.chartColX.value, index++),
                y: _chartNumber(row, paragraph.chartColY.value, 0)
            }
        });

        return [{key: key, values: values}];
    }

    function _insertChart(paragraph, datum, chart) {
        var chartId = 'chart-' + paragraph.id;

        var xAxisLabel = 'X';
        var yAxisLabel = 'Y';

        _.forEach(paragraph.chartColumns, function (col) {
            if (col == paragraph.chartColX)
                xAxisLabel = col.label;

            if (col == paragraph.chartColY)
                yAxisLabel = col.label;
        });

        $timeout(function() {
            chart.height(400);

            if (chart.xAxis)
                chart.xAxis.axisLabel(xAxisLabel);

            if (chart.yAxis)
                chart.yAxis.axisLabel(yAxisLabel);

            // Remove previous chart.
            d3.selectAll('#' + chartId + ' svg > *').remove();

            // Insert new chart.
            d3.select('#' + chartId + ' svg')
                .datum(datum)
                .call(chart)
                .attr('height', 400);

            chart.update();
        });
    }

    $scope.applyChartSettings = function (paragraph) {
        if (paragraph.chart() && paragraph.nonEmpty()) {
            switch (paragraph.result) {
                case 'bar':
                    _barChart(paragraph);
                    break;

                case 'pie':
                    _pieChart(paragraph);
                    break;

                case 'line':
                    _lineChart(paragraph);
                    break;

                case 'area':
                    _areaChart(paragraph);
                    break;
            }
        }
    };

    function _barChart(paragraph) {
        var index = 0;

        nv.addGraph(function() {
            var chart = nv.models.discreteBarChart()
                .x(function (d) { return d.label; })
                .y(function (d) { return d.value;})
                .margin({left: 70});

            var values = _.map(paragraph.rows, function (row) {
                return {
                    label: _chartLabel(row, paragraph.chartColX.value, index++),
                    value: _chartNumber(row, paragraph.chartColY.value, 0)
                }
            });

            _insertChart(paragraph, [{key: 'bar', values: values}], chart);
        });
    }

    function _pieChart(paragraph) {
        var index = 0;

        nv.addGraph(function() {
            var chart = nv.models.pieChart()
                    .x(function (row) {
                        return _chartLabel(row, paragraph.chartColX.value, index++);
                    })
                    .y(function (row) {
                        return _chartNumber(row, paragraph.chartColY.value, 0);
                    })
                .showLabels(true)
                .labelThreshold(.05)
                .labelType("percent")
                .donut(true)
                .donutRatio(0.35);

            _insertChart(paragraph, paragraph.rows, chart);
        });
    }

    function _x(d) {
        return d.x;
    }

    function _y(d) {
        return d.y;
    }

    function _lineChart(paragraph) {
        nv.addGraph(function() {
            var chart = nv.models.lineChart()
                .x(_x)
                .y(_y)
                .margin({left: 70});

            _insertChart(paragraph, _chartDatum('Line chart', paragraph), chart);
        });
    }

    function _areaChart(paragraph) {
        nv.addGraph(function() {
            var chart = nv.models.stackedAreaChart()
                .x(_x)
                .y(_y)
                .margin({left: 70});

            _insertChart(paragraph, _chartDatum('Area chart', paragraph), chart);
        });
    }

    $scope.actionAvailable = function (paragraph, needQuery) {
        return $scope.caches.length > 0 && paragraph.cache && (!needQuery || paragraph.query);
    };

    $scope.actionTooltip = function (paragraph, action, needQuery) {
        return $scope.actionAvailable(paragraph, needQuery) ? undefined
            : 'To ' + action + ' query select cache' + (needQuery ? ' and input query' : '');
    };

    $scope.clickableMetadata = function (node) {
        return node.type.slice(0, 5) != 'index';
    };

    $scope.dblclickMetadata = function (paragraph, node) {
        paragraph.ace.insert(node.name);
        var position = paragraph.ace.selection.getCursor();

        paragraph.query = paragraph.ace.getValue();

        setTimeout(function () {
            paragraph.ace.selection.moveCursorToPosition(position);
        }, 1);
    };

    $scope.tryLoadMetadata = function (cache) {
        if (!cache.metadata) {
            $loading.start('loadingCacheMetadata');

            $http.post('/agent/cache/metadata', {cacheName: cache.name})
                .success(function (metadata) {
                    cache.metadata = metadata;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                })
                .finally(function() {
                    $loading.finish('loadingCacheMetadata');
                });
        }
    }
}]);

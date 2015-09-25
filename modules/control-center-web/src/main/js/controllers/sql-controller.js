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

    var chartSettingsParagraph = null;

    $scope.chartSettingsDragStart = function (paragraph) {
        chartSettingsParagraph = paragraph;
    };

    $scope.removeKeyColumn = function (paragraph, index) {
        paragraph.chartKeyCols.splice(index, 1);

        $scope.applyChartSettings(paragraph);
    };

    $scope.removeValColumn = function (paragraph, index) {
        paragraph.chartValCols.splice(index, 1);

        $scope.applyChartSettings(paragraph);
    };

    function acceptColumn(cols, droppedCol) {
        var accepted = _.findIndex(cols, function (col) {
                return col.label == droppedCol.label;
            }) < 0;

        if (accepted)
            $timeout(function () {
                $scope.applyChartSettings(chartSettingsParagraph);
            });

        return accepted ? droppedCol : false;
    }

    $scope.acceptKeyColumn = function(event, index, item, external, type, allowedType) {
        return acceptColumn(chartSettingsParagraph.chartKeyCols, item);
    };

    $scope.acceptValColumn = function(event, index, item, external, type, allowedType) {
        return acceptColumn(chartSettingsParagraph.chartValCols, item);
    };

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

    $scope.treeOptions = {
        nodeChildren: "children",
        dirSelectable: false,
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

        paragraph.hasChartColumns = function () {
            return !$common.isEmptyArray(this.chartKeyCols) && !$common.isEmptyArray(this.chartValCols);
        };

        Object.defineProperty(paragraph, 'gridOptions', { value: {
            enableColResize: true,
            columnDefs: [
                {headerName: "Test", valueGetter: 'data[0]'}
            ],
            rowData: null
        }});
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

    var _setActiveCache = function () {
        if ($scope.caches.length > 0)
            _.forEach($scope.notebook.paragraphs, function (paragraph) {
                if (!paragraph.cacheName || !_.find($scope.caches, {name: paragraph.cacheName}))
                    paragraph.cacheName = $scope.caches[0].name;
            });
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

                _setActiveCache();
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
        $confirm.confirm('Are you sure you want to remove: "' + $scope.notebook.name + '"?')
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
            paragraph.cacheName = $scope.caches[0].name;

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
        $confirm.confirm('Are you sure you want to remove: "' + paragraph.name + '"?')
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

            _setActiveCache();
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

    var _rebuildColumns = function (paragraph) {
        var columnDefs = [];

        _.forEach(res.meta, function (meta, idx) {
            if (paragraph.columnFilter(meta)) {
                paragraph.chartColumns.push({value: idx + 1, label: meta.fieldName});

                columnDefs.push({
                    headerName: meta.fieldName,
                    valueGetter: 'JSON.stringify(data[' + (idx) + '])'
                });
            }
        });

        paragraph.gridOptions.api.setColumnDefs(columnDefs);

        paragraph.chartKeyCols = _retainColumns(paragraph.chartColumns, paragraph.chartKeyCols, 0);
        paragraph.chartValCols = _retainColumns(paragraph.chartColumns, paragraph.chartValCols, 1);
    };

    $scope.toggleSystemColumns = function (paragraph) {
        if (paragraph.disabledSystemColumns)
            return;

        paragraph.systemColumns = !paragraph.systemColumns;

        paragraph.columnFilter = _columnFilter(paragraph);

        _rebuildColumns(paragraph);

        setTimeout(function () {
            paragraph.gridOptions.api.sizeColumnsToFit();
        });
    };

    function _retainColumns(allCols, curCols, dfltIdx) {
        var retainedCols = [];

        var allColsLen = allCols.length;

        if (allColsLen > 0) {
            curCols.forEach(function (curCol) {
                var idx = _.findIndex(allCols, function (allCol) {
                    return allCol.label == curCol.label;
                });

                if (idx >= 0)
                    retainedCols.push(allCols[idx]);
            });

            if ($common.isEmptyArray(retainedCols))
                retainedCols.push(allCols[dfltIdx < allColsLen ? dfltIdx : 0]);
        }

        return retainedCols;
    }

    var _processQueryResult = function (paragraph) {
        return function (res) {
            paragraph.meta = [];
            paragraph.chartColumns = [];

            if (!$common.isDefined(paragraph.chartKeyCols))
                paragraph.chartKeyCols = [];

            if (!$common.isDefined(paragraph.chartValCols ))
                paragraph.chartValCols = [];

            if (res.meta) {
                paragraph.disabledSystemColumns = res.meta.length == 2 &&
                    res.meta[0].fieldName === "_KEY" && res.meta[1].fieldName === "_VAL";

                paragraph.columnFilter = _columnFilter(paragraph);

                paragraph.meta = res.meta;

                _rebuildColumns(paragraph)
            }

            paragraph.page = 1;

            paragraph.total = 0;

            paragraph.queryId = res.queryId;

            delete paragraph.errMsg;

            paragraph.rows = res.rows;

            paragraph.gridOptions.api.showLoading(false);

            paragraph.gridOptions.api.setRowData(res.rows);

            setTimeout(function () {
                paragraph.gridOptions.api.sizeColumnsToFit();
            });

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

        paragraph.gridOptions.api.showLoading(true);

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

        paragraph.gridOptions.api.showLoading(true);

        $http.post('/agent/query', {query: 'EXPLAIN ' + paragraph.query, pageSize: paragraph.pageSize, cacheName: paragraph.cacheName})
            .success(_processQueryResult(paragraph))
            .error(function (errMsg) {
                paragraph.errMsg = errMsg;
            });
    };

    $scope.scan = function (paragraph) {
        _saveNotebook();

        _cancelRefresh(paragraph);

        paragraph.gridOptions.api.showLoading(true);

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

    function _chartDatumLblNum(paragraph) {
        var datum = [];

        if (paragraph.hasChartColumns()) {
            paragraph.chartValCols.forEach(function (valCol) {
                var index = 0;

                var values = _.map(paragraph.rows, function (row) {
                    return {
                        lbl: _chartLabel(row, paragraph.chartKeyCols[0].value, index++),
                        val: _chartNumber(row, valCol.value, 0)
                    }
                });

                datum.push({key: valCol.label, values: values});
            });
        }

        return datum;
    }


    function _chartDatumNumNum(paragraph) {
        var datum = [];

        if (paragraph.hasChartColumns()) {
             paragraph.chartValCols.forEach(function (valCol) {
                 var index = 0;

                 var values = _.map(paragraph.rows, function (row) {
                     return {
                         x: _chartNumber(row, paragraph.chartKeyCols[0].value, index++),
                         y: _chartNumber(row, valCol.value, 0)
                     }
                 });

                 datum.push({key: valCol.label, values: values});
             });
         }

        return datum;
    }

    function _colLabel(col) {
        return col.label;
    }

    function _insertChart(paragraph, datum, chart, remove) {
        var chartId = 'chart-' + paragraph.id;

        var xAxisLabel = 'X';
        var yAxisLabel = 'Y';

        if (paragraph.hasChartColumns()) {
            xAxisLabel = _.map(paragraph.chartKeyCols, _colLabel).join(', ');
            yAxisLabel = _.map(paragraph.chartValCols, _colLabel).join(', ');
        }

        $timeout(function() {
            if (remove)
                d3.select('#' + chartId).selectAll('*').remove();

            chart.height(400);

            if (chart.xAxis)
                chart.xAxis.axisLabel(xAxisLabel);

            if (chart.yAxis)
                chart.yAxis.axisLabel(yAxisLabel);

            // Insert new chart.
            d3.select('#' + chartId)
                .append('svg')
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

    function _xLbl(d) {
        return d.lbl;
    }

    function _yVal(d) {
        return d.val;
    }


    function _barChart(paragraph) {
        nv.addGraph(function() {
            var chart = nv.models.multiBarChart()
                .showControls(true) //Allow user to switch between 'Grouped' and 'Stacked' mode.
                .x(_xLbl)
                .y(_yVal)
                .margin({left: 70});

            _insertChart(paragraph, _chartDatumLblNum(paragraph), chart, true);
        });
    }

    function _pieChart(paragraph) {
        var datum = _chartDatumLblNum(paragraph);

        if (datum.length == 0)
            datum = [{key: 'No data', values: []}];

        var first = true;

        datum.forEach(function (d) {
            nv.addGraph(function() {
                var chart = nv.models.pieChart()
                    .x(_xLbl)
                    .y(_yVal)
                    .showLabels(true)
                    .labelThreshold(.05)
                    .labelType("percent")
                    .donut(true)
                    .donutRatio(0.35);

                var datum = [];

                if (paragraph.hasChartColumns())
                    datum = paragraph.rows;

                _insertChart(paragraph, d.values, chart, first);

                first = false;
            });
        });
    }

    function _xX(d) {
        return d.x;
    }

    function _yY(d) {
        return d.y;
    }

    function _lineChart(paragraph) {
        nv.addGraph(function() {
            var chart = nv.models.lineChart()
                .x(_xX)
                .y(_yY)
                .margin({left: 70});

            _insertChart(paragraph, _chartDatumNumNum(paragraph), chart, true);
        });
    }

    function _areaChart(paragraph) {
        nv.addGraph(function() {
            var chart = nv.models.stackedAreaChart()
                .x(_xX)
                .y(_yY)
                .margin({left: 70});

            _insertChart(paragraph, _chartDatumNumNum(paragraph), chart, true);
        });
    }

    $scope.actionAvailable = function (paragraph, needQuery) {
        return $scope.caches.length > 0 && paragraph.cacheName && (!needQuery || paragraph.query);
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
                    cache.metadata = _.sortBy(metadata, 'name');
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

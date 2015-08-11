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

controlCenterModule.controller('metadataController', [
        '$scope', '$http', '$modal', '$common', '$focus', '$confirm', '$copy', '$table', function ($scope, $http, $modal, $common, $focus,
            $confirm, $copy, $table) {
            $scope.joinTip = $common.joinTip;
            $scope.getModel = $common.getModel;
            $scope.javaBuildInClasses = $common.javaBuildInClasses;

            $scope.tableReset = $table.tableReset;
            $scope.tableNewItem = $table.tableNewItem;
            $scope.tableNewItemActive = $table.tableNewItemActive;
            $scope.tableEditing = $table.tableEditing;
            $scope.tableStartEdit = $table.tableStartEdit;
            $scope.tableRemove = $table.tableRemove;

            $scope.tableSimpleSave = $table.tableSimpleSave;
            $scope.tableSimpleSaveVisible = $table.tableSimpleSaveVisible;
            $scope.tableSimpleUp = $table.tableSimpleUp;
            $scope.tableSimpleDown = $table.tableSimpleDown;
            $scope.tableSimpleDownVisible = $table.tableSimpleDownVisible;

            $scope.tablePairSave = $table.tablePairSave;
            $scope.tablePairSaveVisible = $table.tablePairSaveVisible;

            $scope.databases = [
                {value: 'oracle', label: 'Oracle database'},
                {value: 'db2', label: 'IBM DB2'},
                {value: 'mssql', label: 'MS SQL Server'},
                {value: 'postgre', label: 'PostgreSQL'},
                {value: 'mysql', label: 'MySQL'},
                {value: 'h2', label: 'H2 database'}
            ];

            var presets = {
                oracle: {
                    db: 'oracle',
                    drvClass: 'oracle.jdbc.OracleDriver',
                    drvUrl: 'jdbc:oracle:thin:@[host]:[port]:[database]',
                    user: 'system'
                },
                db2: {
                    db: 'db2',
                    drvClass: 'com.ibm.db2.jcc.DB2Driver',
                    drvUrl: 'jdbc:db2://[host]:[port]/[database]',
                    user: 'db2admin'
                },
                mssql: {
                    db: 'mssql',
                    drvClass: 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
                    drvUrl: 'jdbc:sqlserver://[host]:[port][;databaseName=database]',
                    user: 'sa'
                },
                postgre: {
                    db: 'postgre',
                    drvClass: 'org.postgresql.Driver', drvUrl: 'jdbc:postgresql://[host]:[port]/[database]',
                    user: 'sa'
                },
                mysql: {
                    db: 'mysql',
                    drvClass: 'com.mysql.jdbc.Driver',
                    drvUrl: 'jdbc:mysql://[host]:[port]/[database]', user: 'root'
                },
                h2: {
                    db: 'h2,',
                    drvClass: 'org.h2.Driver',
                    drvUrl: 'jdbc:h2:[database]',
                    user: 'sa'
                }
            };

            $scope.preset = presets['oracle'];

            $scope.$watch('preset.db', function (newDb) {
                if (newDb) {
                    var newPreset = presets[newDb];
                    var curPreset = $scope.preset;

                    curPreset.drvClass = newPreset.drvClass;
                    curPreset.drvUrl = newPreset.drvUrl;
                    curPreset.user = newPreset.user;
                }
            }, true);

            $scope.jdbcTypes = $common.mkOptions($common.JDBC_TYPES);

            $scope.javaTypes = $common.mkOptions($common.javaBuildInClasses);

            $scope.sortDirections = [
                {value: false, label: 'ASC'},
                {value: true, label: 'DESC'}
            ];

            $scope.tables = [
                {
                    schemaName: 'Schema1',
                    use: true,
                    tableName: 'Table1',
                    keyClass: 'KeyClass1',
                    valueClass: 'ValueClass1',
                    fields: [
                        {
                            use: true,
                            key: true,
                            ak: true,
                            databaseName: 'name1',
                            databaseType: 'dbType1',
                            javaName: 'javaName1',
                            javaType: 'javaType1'
                        },
                        {
                            use: true,
                            key: false,
                            ak: false,
                            databaseName: 'name2',
                            databaseType: 'dbType2',
                            javaName: 'javaName2',
                            javaType: 'javaType2'
                        },
                        {
                            use: false,
                            key: false,
                            ak: false,
                            databaseName: 'name3',
                            databaseType: 'dbType3',
                            javaName: 'javaName3',
                            javaType: 'javaType3'
                        }
                    ]
                },
                {
                    schemaName: 'Schema with very long name',
                    use: false,
                    tableName: 'Table2',
                    keyClass: 'KeyClass2',
                    valueClass: 'ValueClass2',
                    fields: [
                        {
                            use: true,
                            key: true,
                            ak: true,
                            databaseName: 'name4',
                            databaseType: 'dbType4',
                            javaName: 'javaName4',
                            javaType: 'javaType4'
                        },
                        {
                            use: true,
                            key: false,
                            ak: false,
                            databaseName: 'name5',
                            databaseType: 'dbType5',
                            javaName: 'javaName5',
                            javaType: 'javaType5'
                        },
                        {
                            use: false,
                            key: false,
                            ak: false,
                            databaseName: 'name6',
                            databaseType: 'dbType6',
                            javaName: 'javaName6',
                            javaType: 'javaType6'
                        }
                    ]
                },
                {
                    schemaName: 'Schema3',
                    use: false,
                    tableName: 'Table3',
                    keyClass: 'KeyClass3',
                    valueClass: 'ValueClass3',
                    fields: [
                        {
                            use: true,
                            key: true,
                            ak: true,
                            databaseName: 'name7',
                            databaseType: 'dbType7',
                            javaName: 'javaName7',
                            javaType: 'javaType7'
                        },
                        {
                            use: true,
                            key: false,
                            ak: false,
                            databaseName: 'name8',
                            databaseType: 'dbType8',
                            javaName: 'javaName8',
                            javaType: 'javaType8'
                        },
                        {
                            use: false,
                            key: false,
                            ak: false,
                            databaseName: 'name9',
                            databaseType: 'dbType9',
                            javaName: 'javaName9',
                            javaType: 'javaType9'
                        },
                        {
                            use: false,
                            key: false,
                            ak: false,
                            databaseName: 'name10',
                            databaseType: 'dbType10',
                            javaName: 'javaName10',
                            javaType: 'javaType10'
                        },
                        {
                            use: false,
                            key: false,
                            ak: false,
                            databaseName: 'name11',
                            databaseType: 'dbType11',
                            javaName: 'javaName11',
                            javaType: 'javaType11'
                        },
                        {
                            use: false,
                            key: false,
                            ak: false,
                            databaseName: 'name12',
                            databaseType: 'dbType12',
                            javaName: 'javaName12',
                            javaType: 'javaType12'
                        }
                    ]
                }];

            $scope.metadatas = [];

            $scope.isJavaBuildInClass = function () {
                var item = $scope.backupItem;

                if (item && item.keyType)
                    return $common.isJavaBuildInClass(item.keyType);

                return false;
            };

            $http.get('/models/metadata.json')
                .success(function (data) {
                    $scope.screenTip = data.screenTip;
                    $scope.templateTip = data.templateTip;
                    $scope.metadata = data.metadata;
                    $scope.metadataDb = data.metadataDb;
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            function selectFirstItem() {
                if ($scope.metadatas.length > 0)
                    $scope.selectItem($scope.metadatas[0]);
            }

            function setSelectedAndBackupItem(sel, bak) {
                $table.tableReset();

                $scope.selectedItem = sel;
                $scope.backupItem = bak;
            }

            // Pre-fetch modal dialogs.
            var loadMetaModal = $modal({scope: $scope, templateUrl: 'metadata/metadata-load', show: false});

            // Show load metadata modal.
            $scope.loadFromDb = function () {
                $http.post('/agent/drivers')
                    .success(function (drivers) {
                        if (drivers && drivers.length > 0) {
                            $scope.drivers = _.map(drivers, function (driver) {
                                return {value: driver, label: driver};
                            });

                            $scope.preset.drvJar = drivers[0];

                            loadMetaModal.$promise.then(function () {
                                loadMetaModal.show();

                                $focus('db');
                            });
                        }
                        else
                            $common.showError('JDBC drivers not found!');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            };

            $scope.saveMetadataLoadedFromDb = function(preset) {
                loadMetaModal.hide();

                $common.showError("Load metadata from DB not ready yet!");
            };

            // When landing on the page, get metadatas and show them.
            $http.post('metadata/list')
                .success(function (data) {
                    $scope.spaces = data.spaces;
                    $scope.metadatas = data.metadatas;

                    var restoredItem = angular.fromJson(sessionStorage.metadataBackupItem);

                    if (restoredItem) {
                        if (restoredItem._id) {
                            var idx = _.findIndex($scope.metadatas, function (metadata) {
                                return metadata._id == restoredItem._id;
                            });

                            if (idx >= 0) {
                                // Remove deleted metadata.
                                restoredItem.queryMetadata = _.filter(restoredItem.queryMetadata, function (metaId) {
                                    return _.findIndex($scope.metadatas, function (scopeMeta) {
                                            return scopeMeta.value == metaId;
                                        }) >= 0;
                                });

                                // Remove deleted metadata.
                                restoredItem.storeMetadata = _.filter(restoredItem.storeMetadata, function (metaId) {
                                    return _.findIndex($scope.metadatas, function (scopeMeta) {
                                            return scopeMeta.value == metaId;
                                        }) >= 0;
                                });

                                setSelectedAndBackupItem($scope.metadatas[idx], restoredItem);
                            }
                            else {
                                sessionStorage.removeItem('metadataBackupItem');

                                selectFirstItem();
                            }
                        }
                        else
                            setSelectedAndBackupItem(undefined, restoredItem);
                    }
                    else
                        selectFirstItem();

                    $scope.$watch('backupItem', function (val) {
                        if (val)
                            sessionStorage.metadataBackupItem = angular.toJson(val);
                    }, true);
                })
                .error(function (errMsg) {
                    $common.showError(errMsg);
                });

            $scope.selectItem = function (item) {
                setSelectedAndBackupItem(item, angular.copy(item));
            };

            // Add new metadata.
            $scope.createItem = function () {
                $table.tableReset();

                $scope.selectedItem = undefined;

                $scope.backupItem = {space: $scope.spaces[0]._id};
            };

            // Check metadata logical consistency.
            function validate(item) {
                /*
                if (!$common.isValidJavaClass('Key type', item.keyType, true)) {
                    $focus('keyType');

                    return false;
                }

                if (!$common.isValidJavaClass('Value type', item.valueType, false)) {
                    $focus('valueType');

                    return false;
                }

                if ($common.isEmptyArray(item.queryFields) && $common.isEmptyArray(item.ascendingFields) &&
                    $common.isEmptyArray(item.descendingFields) && $common.isEmptyArray(item.textFields) &&
                    $common.isEmptyArray(item.groups)) {
                    $common.showError('SQL fields are not specified!');

                    return false;
                }

                var groups = item.groups;
                if (groups && groups.length > 0) {
                    for (var i = 0; i < groups.length; i++) {
                        var group = groups[i];
                        var fields = group.fields;

                        if ($common.isEmptyArray(fields)) {
                            $common.showError('Group "' + group.name + '" has no fields.');

                            return false;
                        }

                        if (fields.length == 1) {
                            $common.showError('Group "' + group.name + '" has only one field.<br/> Consider to use ascending or descending fields.');

                            return false;
                        }
                    }
                }

                if ($common.isEmptyArray(item.keyFields) && !$common.isJavaBuildInClass(item.keyType)) {
                    $common.showError('Key fields are not specified!');

                    return false;
                }
                if ($common.isEmptyArray(item.valueFields)) {
                    $common.showError('Value fields are not specified!');

                    return false;
                }
                */

                return true;
            }

            // Save cache type metadata into database.
            function save(item) {
                $http.post('metadata/save', item)
                    .success(function (_id) {
                        $common.showInfo('Metadata "' + item.name + '" saved.');

                        var idx = _.findIndex($scope.metadatas, function (metadata) {
                            return metadata._id == _id;
                        });

                        if (idx >= 0)
                            angular.extend($scope.metadatas[idx], item);
                        else {
                            item._id = _id;

                            $scope.metadatas.push(item);
                        }

                        $scope.selectItem(item);

                        $common.showInfo('Cache type metadata"' + item.name + '" saved.');
                    })
                    .error(function (errMsg) {
                        $common.showError(errMsg);
                    });
            }

            // Save cache type metadata.
            $scope.saveItem = function () {
                $table.tableReset();

                var item = $scope.backupItem;

                if (validate(item))
                    save(item);
            };

            // Save cache type metadata with new name.
            $scope.saveItemAs = function () {
                $table.tableReset();

                if (validate($scope.backupItem))
                    $copy.show($scope.backupItem.name).then(function (newName) {
                        var item = angular.copy($scope.backupItem);

                        item._id = undefined;
                        item.name = newName;

                        save(item);
                    });
            };

            $scope.removeItem = function () {
                $table.tableReset();

                var selectedItem = $scope.selectedItem;

                $confirm.show('Are you sure you want to remove cache type metadata: "' + selectedItem.name + '"?').then(
                    function () {
                        var _id = selectedItem._id;

                        $http.post('metadata/remove', {_id: _id})
                            .success(function () {
                                $common.showInfo('Cache type metadata has been removed: ' + selectedItem.name);

                                var metadatas = $scope.metadatas;

                                var idx = _.findIndex(metadatas, function (metadata) {
                                    return metadata._id == _id;
                                });

                                if (idx >= 0) {
                                    metadatas.splice(idx, 1);

                                    if (metadatas.length > 0)
                                        $scope.selectItem(metadatas[0]);
                                    else {
                                        $scope.selectedItem = undefined;
                                        $scope.backupItem = undefined;
                                    }
                                }
                            })
                            .error(function (errMsg) {
                                $common.showError(errMsg);
                            });
                    });
            };

            function focusInvalidField(index, id) {
                $focus(index < 0 ? 'new' + id : 'cur' + id);

                return false;
            }

            $scope.tableSimpleValid = function (item, field, name, index) {
                var model = item[field.model];

                if ($common.isDefined(model)) {
                    var idx = _.indexOf(model, name);

                    // Found duplicate.
                    if (idx >= 0 && idx != index) {
                        $common.showError('Field with such name already exists!');

                        return focusInvalidField(index, 'TextField');
                    }
                }

                return true;
            };

            var pairFields = {
                queryFields: {msg: 'Query field class', id: 'QryField'},
                ascendingFields: {msg: 'Ascending field class', id: 'AscField'},
                descendingFields: {msg: 'Descending field class', id: 'DescField'}
            };

            $scope.tablePairValid = function (item, field, name, clsName, index) {
                var pairField = pairFields[field.model];

                if (pairField) {
                    if (!$common.isValidJavaClass(pairField.msg, clsName, true))
                        return focusInvalidField(index, 'Value' + pairField.id);

                    var model = item[field.model];

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (pair) {
                            return pair.name == name
                        });

                        // Found duplicate.
                        if (idx >= 0 && idx != index) {
                            $common.showError('Field with such name already exists!');

                            return focusInvalidField(index, 'Key' + pairField.id);
                        }
                    }
                }

                return true;
            };

            $scope.tableDbFieldSaveVisible = function (databaseName, databaseType, javaName, javaType) {
                return !$common.isEmptyString(databaseName) && $common.isDefined(databaseType) && !$common.isEmptyString(javaName) && $common.isDefined(javaType);
            };

            var dbFields = {
                keyFields: {msg: 'Key field', id: 'KeyField'},
                valueFields: {msg: 'Value field', id: 'ValueField'}
            };

            $scope.tableDbFieldSave = function (field, newDatabaseName, newDatabaseType, newJavaName, newJavaType,
                index) {
                var dbField = dbFields[field.model];

                if (dbField) {
                    var backupItem = $scope.backupItem;

                    var model = backupItem[field.model];

                    var newItem = {
                        databaseName: newDatabaseName,
                        databaseType: newDatabaseType,
                        javaName: newJavaName,
                        javaType: newJavaType
                    };

                    if (!$common.isValidJavaIdentifier(dbField.msg + ' java name', newJavaName))
                        return focusInvalidField(index, 'JavaName' + dbField.id);

                    if ($common.isDefined(model)) {
                        var idx = _.findIndex(model, function (dbMeta) {
                            return dbMeta.databaseName == newDatabaseName
                        });

                        // Found duplicate.
                        if (idx >= 0 && index != idx) {
                            $common.showError('Field with such database name already exists!');

                            return focusInvalidField(index, 'DatabaseName' + dbField.id);
                        }

                        idx = _.findIndex(model, function (dbMeta) {
                            return dbMeta.javaName == newJavaName
                        });

                        // Found duplicate.
                        if (idx >= 0 && index != idx) {
                            $common.showError('Field with such java name already exists!');

                            return focusInvalidField(index, 'JavaName' + dbField.id);
                        }

                        if (index < 0) {
                            if (model)
                                model.push(newItem);
                            else
                                backupItem[field.model] = [newItem];
                        }
                        else {
                            var item = model[index];

                            item.databaseName = newDatabaseName;
                            item.databaseType = newDatabaseType;
                            item.javaName = newJavaName;
                            item.javaType = newJavaType;
                        }
                    }
                    else
                        backupItem[field.model] = [newItem];

                    $table.tableReset();
                }
            };

            $scope.tableGroupSaveVisible = function (group) {
                return !$common.isEmptyString(group);
            };

            function tableGroupValid(groupName, index) {
                var groups = $scope.backupItem.groups;

                if ($common.isDefined(groups)) {
                    var idx = _.findIndex(groups, function (group) {
                        return group.name == groupName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index) {
                        $common.showError('Group with such name already exists!');

                        return focusInvalidField(index, 'GroupName');
                    }
                }

                return true;
            }

            $scope.tableGroupSave = function (groupName, index) {
                if (tableGroupValid(groupName, index)) {
                    $table.tableReset();

                    var item = $scope.backupItem;

                    if (index < 0) {
                        var newGroup = {name: groupName};

                        if (item.groups)
                            item.groups.push(newGroup);
                        else
                            item.groups = [newGroup];
                    }
                    else
                        item.groups[index].name = groupName;
                }
            };

            $scope.tableGroupNewItem = function (groupIndex) {
                var groupName = $scope.backupItem.groups[groupIndex].name;

                return $table.tableNewItem({model: groupName});
            };

            $scope.tableGroupNewItemActive = function (groupIndex) {
                var groups = $scope.backupItem.groups;

                if (groups) {
                    var group = groups[groupIndex];

                    if (group) {
                        var groupName = group.name;

                        return $table.tableNewItemActive({model: groupName});
                    }
                }

                return false;
            };

            $scope.tableGroupItemEditing = function (groupIndex, index) {
                var groups = $scope.backupItem.groups;

                if (groups) {
                    var group = groups[groupIndex];

                    if (group)
                        return $table.tableEditing({model: group.name}, index);
                }

                return false;
            };

            $scope.tableGroupItemStartEdit = function (groupIndex, index) {
                var groups = $scope.backupItem.groups;

                $table.tableState(groups[groupIndex].name, index);

                return groups[groupIndex].fields[index];
            };

            $scope.tableGroupItemSaveVisible = function (fieldName, className) {
                return !$common.isEmptyString(fieldName) && !$common.isEmptyString(className);
            };

            function tableGroupItemValid(fieldName, className, groupIndex, index) {
                if (!$common.isValidJavaClass('Group field', className, true))
                    return focusInvalidField(index, 'FieldClassName');

                var fields = $scope.backupItem.groups[groupIndex].fields;

                if ($common.isDefined(fields)) {
                    var idx = _.findIndex(fields, function (field) {
                        return field.name == fieldName;
                    });

                    // Found duplicate.
                    if (idx >= 0 && idx != index) {
                        $common.showError('Field with such name already exists in group!');

                        return focusInvalidField(index, 'FieldName');
                    }
                }

                return true;
            }

            $scope.tableGroupItemSave = function (fieldName, className, direction, groupIndex, index) {
                if (tableGroupItemValid(fieldName, className, groupIndex, index)) {
                    $table.tableReset();

                    var group = $scope.backupItem.groups[groupIndex];

                    if (index < 0) {
                        var newGroupItem = {name: fieldName, className: className, direction: direction};

                        if (group.fields)
                            group.fields.push(newGroupItem);
                        else
                            group.fields = [newGroupItem];
                    }
                    else {
                        var groupItem = group.fields[index];

                        groupItem.name = fieldName;
                        groupItem.className = className;
                        groupItem.direction = direction;
                    }
                }
            };

            $scope.tableRemoveGroupItem = function (group, index) {
                $table.tableReset();

                group.fields.splice(index, 1);
            };

            //$scope.selectSchema = function (idx) {
            //    var data = $scope.data;
            //    var tables = data.tables;
            //    var schemaName = tables[idx].schemaName;
            //    var use = tables[idx].use;
            //
            //    for (var i = idx + 1; i < tables.length; i++) {
            //        var item = tables[i];
            //
            //        if (item.schemaName == schemaName && item.tableName)
            //            item.use = use;
            //        else
            //            break;
            //    }
            //
            //    data.curTableIdx = -1;
            //    data.curFieldIdx = -1;
            //};

            //$scope.selectTable = function (idx) {
            //    var data = $scope.data;
            //
            //    data.curTableIdx = idx;
            //    data.curFieldIdx = -1;
            //
            //    if (idx >= 0) {
            //        var tbl = data.tables[idx];
            //
            //        data.curKeyClass = tbl.keyClass;
            //        data.curValueClass = tbl.valueClass;
            //    }
            //};
            //
            //$scope.selectField = function (idx) {
            //    var data = $scope.data;
            //
            //    data.curFieldIdx = idx;
            //
            //    if (idx >= 0) {
            //        var fld = data.tables[data.curTableIdx].fields[idx];
            //
            //        data.curJavaName = fld.javaName;
            //        data.curJavaType = fld.javaType;
            //    }
            //};
        }]
)
;

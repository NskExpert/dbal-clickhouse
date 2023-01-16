<?php
/*
 * This file is part of the FODDBALClickHouse package -- Doctrine DBAL library
 * for ClickHouse (a column-oriented DBMS for OLAP <https://clickhouse.yandex/>)
 *
 * (c) FriendsOfDoctrine <https://github.com/FriendsOfDoctrine/>.
 *
 * For the full copyright and license inflormation, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace FOD\DBALClickHouse;

use Doctrine\DBAL\DBALException;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\TrimMode;
use Doctrine\DBAL\Schema\ColumnDiff;
use Doctrine\DBAL\Types\BlobType;
use Doctrine\DBAL\Types\DecimalType;
use Doctrine\DBAL\Types\FloatType;
use Doctrine\DBAL\Types\StringType;
use Doctrine\DBAL\Types\TextType;
use Doctrine\DBAL\Types\Type;
use Doctrine\DBAL\Types\DateType;
use Doctrine\DBAL\Types\IntegerType;
use Doctrine\DBAL\Types\SmallIntType;
use Doctrine\DBAL\Types\BigIntType;
use Doctrine\DBAL\Types\DateTimeType;

use Doctrine\DBAL\Schema\Index;
use Doctrine\DBAL\Schema\ForeignKeyConstraint;
use Doctrine\DBAL\Schema\TableDiff;
use Exception;
use InvalidArgumentException;
use PDO;

/**
 * Provides the behavior, features and SQL dialect of the ClickHouse database platform.
 *
 * @author Mochalygin <a@mochalygin.ru>
 */
class ClickHousePlatform extends AbstractPlatform
{
    /**
     * {@inheritDoc}
     */
    public function getBooleanTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $columnDef
    ) {
        return 'UInt8';
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    public function getIntegerTypeDeclarationSQL(array $columnDef)
    {
        return $this->_getCommonIntegerTypeDeclarationSQL($columnDef) . 'Int32';
    }

    /**
     * {@inheritDoc}
     */
    public function getBigIntTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $columnDef
    ) {
        return 'String';
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    public function getSmallIntTypeDeclarationSQL(array $columnDef)
    {
        return 'Int16' . $this->_getCommonIntegerTypeDeclarationSQL($columnDef);
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    protected function _getCommonIntegerTypeDeclarationSQL(array $columnDef)
    {
        if (!empty($columnDef['autoincrement'])) {
            throw new Exception('Clickhouse do not support AUTO_INCREMENT fields');
        }

        return empty($columnDef['unsigned']) ? '' : 'U';
    }

    /**
     * {@inheritDoc}
     */
    protected function initializeDoctrineTypeMappings()
    {
        $this->doctrineTypeMapping = [
            'int8' => 'smallint',
            'int16' => 'integer',
            'int32' => 'integer',
            'int64' => 'bigint',
            'uint8' => 'smallint',
            'uint16' => 'integer',
            'uint32' => 'integer',
            'uint64' => 'bigint',
            'float32' => 'decimal',
            'float64' => 'float',
            'string' => 'string',
            'fixedstring' => 'string',
            'date' => 'date',
            'datetime' => 'datetime',
            'array(int8)' => 'array',
            'array(int16)' => 'array',
            'array(int32)' => 'array',
            'array(int64)' => 'array',
            'array(uint8)' => 'array',
            'array(uint16)' => 'array',
            'array(uint32)' => 'array',
            'array(uint64)' => 'array',
            'array(float32)' => 'array',
            'array(float64)' => 'array',
            'array(string)' => 'array',
            'array(date)' => 'array',
            'array(datetime)' => 'array',
            'enum8' => 'string',
            'enum16' => 'string',
        ];
    }

    /**
     * {@inheritDoc}
     */
    protected function getVarcharTypeDeclarationSQLSnippet($length, $fixed)
    {
        return $fixed
            ? 'FixedString(' . $length . ')'
            : 'String';
    }

    /**
     * {@inheritDoc}
     */
    protected function getBinaryTypeDeclarationSQLSnippet(
        /** @noinspection PhpUnusedParameterInspection */
        $length,
        $fixed
    ) {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */
    public function getClobTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $field
    ) {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */
    public function getBlobTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $field
    ) {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */
    public function getName()
    {
        return 'clickhouse';
    }

    /**
     * {@inheritDoc}
     */
    public function getIdentifierQuoteCharacter()
    {
        return '`';
    }

    /**
     * {@inheritDoc}
     */
    public function getVarcharDefaultLength()
    {
        return 512;
    }

    /**
     * {@inheritDoc}
     */
    public function getCountExpression(
        /** @noinspection PhpUnusedParameterInspection */
        $column
    ) {
        return 'COUNT()';
    }

    // scalar functions

    /**
     * {@inheritDoc}
     */
    public function getMd5Expression($column)
    {
        return 'MD5(CAST(' . $column . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getLengthExpression($column)
    {
        return 'lengthUTF8(CAST(' . $column . ' AS String))';
    }

    /**
     * {@inheritDoc}
     */
    public function getSqrtExpression($column)
    {
        return 'sqrt(' . $column . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getRoundExpression($column, $decimals = 0)
    {
        return 'round(' . $column . ', ' . $decimals . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getModExpression($expression1, $expression2)
    {
        return 'modulo(' . $expression1 . ', ' . $expression2 . ')';
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getTrimExpression(
        /** @noinspection PhpUnusedParameterInspection */
        $str,
        $pos = TrimMode::UNSPECIFIED,
        $char = false
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getRtrimExpression(
        /** @noinspection PhpUnusedParameterInspection */
        $str
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getLtrimExpression(
        /** @noinspection PhpUnusedParameterInspection */
        $str
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getUpperExpression($str)
    {
        return 'upperUTF8(' . $str . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getLowerExpression($str)
    {
        return 'lowerUTF8(' . $str . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getLocateExpression(
        $str,
        $substr,
        /** @noinspection PhpUnusedParameterInspection */
        $startPos = false
    ) {
        return 'positionUTF8(' . $str . ', ' . $substr . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getNowExpression()
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getSubstringExpression($value, $from, $length = null)
    {
        if (null === $length) {
            throw new InvalidArgumentException("'length' argument must be a constant");
        }

        return 'substringUTF8(' . $value . ', ' . $from . ', ' . $length . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getConcatExpression()
    {
        return 'concat(' . implode(', ', func_get_args()) . ')';
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getIsNullExpression(
        /** @noinspection PhpUnusedParameterInspection */
        $expression
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getIsNotNullExpression(
        /** @noinspection PhpUnusedParameterInspection */
        $expression
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAcosExpression($value)
    {
        return 'acos(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getSinExpression($value)
    {
        return 'sin(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getPiExpression()
    {
        return 'pi()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCosExpression($value)
    {
        return 'cos(' . $value . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateDiffExpression($date1, $date2)
    {
        return 'CAST(' . $date1 . ' AS Date) - CAST(' . $date2 . ' AS Date)';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddSecondsExpression($date, $seconds)
    {
        return $date . ' + ' . $seconds;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubSecondsExpression($date, $seconds)
    {
        return $date . ' - ' . $seconds;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddMinutesExpression($date, $minutes)
    {
        return $date . ' + ' . $minutes * 60;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubMinutesExpression($date, $minutes)
    {
        return $date . ' - ' . $minutes * 60;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddHourExpression($date, $hours)
    {
        return $date . ' + ' . $hours * 60 * 60;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubHourExpression($date, $hours)
    {
        return $date . ' - ' . $hours * 60 * 60;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddDaysExpression($date, $days)
    {
        return $date . ' + ' . $days * 60 * 60 * 24;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubDaysExpression($date, $days)
    {
        return $date . ' - ' . $days * 60 * 60 * 24;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateAddWeeksExpression($date, $weeks)
    {
        return $date . ' + ' . $weeks * 60 * 60 * 24 * 7;
    }

    /**
     * {@inheritDoc}
     */
    public function getDateSubWeeksExpression($date, $weeks)
    {
        return $date . ' - ' . $weeks * 60 * 60 * 24 * 7;
    }

    /**
     * {@inheritDoc}
     */
    public function getBitAndComparisonExpression($value1, $value2)
    {
        return 'bitAnd(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     */
    public function getBitOrComparisonExpression($value1, $value2)
    {
        return 'bitOr(' . $value1 . ', ' . $value2 . ')';
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getForUpdateSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @param $fromClause
     * @param $lockMode
     * @return string
     */
    public function appendLockHint($fromClause, $lockMode): string
    {
        return $fromClause;
    }

    /**
     * {@inheritDoc}
     */
    public function getReadLockSQL()
    {
        //throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getWriteLockSQL()
    {
        //throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getDropIndexSQL($index, $table = null)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getDropConstraintSQL($constraint, $table)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getDropForeignKeySQL(
        /** @noinspection PhpUnusedParameterInspection */
        $foreignKey,
        $table
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getCommentOnColumnSQL($tableName, $columnName, $comment)
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws Exception
     */
    protected function _getCreateTableSQL($tableName, array $columns, array $options = [])
    {
        $engine = !empty($options['engine']) ? $options['engine'] : 'ReplacingMergeTree';
        $engineOptions = '';

        /**
         * MergeTree* specific section
         */
        if (in_array($engine,
            ['MergeTree', 'CollapsingMergeTree', 'SummingMergeTree', 'AggregatingMergeTree', 'ReplacingMergeTree'],
            true)) {
            $indexGranularity = !empty($options['indexGranularity']) ? $options['indexGranularity'] : 8192;

            /**
             * eventDateColumn section
             */
            $dateColumnParams = [
                'type' => Type::getType('date'),
                'default' => 'today()',
            ];
            if (!empty($options['eventDateProviderColumn'])) {
                $options['eventDateProviderColumn'] = trim($options['eventDateProviderColumn']);
                if (!isset($columns[$options['eventDateProviderColumn']])) {
                    throw new Exception('Table `' . $tableName . '` has not column with name: `' . $options['eventDateProviderColumn']);
                }

                if (
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof DateType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof DateTimeType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof TextType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof IntegerType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof SmallIntType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof BigIntType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof FloatType ||
                    $columns[$options['eventDateProviderColumn']]['type'] instanceof DecimalType ||
                    (
                        $columns[$options['eventDateProviderColumn']]['type'] instanceof StringType &&
                        !$columns[$options['eventDateProviderColumn']]['fixed']
                    )
                ) {
                    $dateColumnParams['default'] =
                        $columns[$options['eventDateProviderColumn']]['type'] instanceof IntegerType ||
                        $columns[$options['eventDateProviderColumn']]['type'] instanceof SmallIntType ||
                        $columns[$options['eventDateProviderColumn']]['type'] instanceof FloatType ?
                            ('toDate(toDateTime(' . $options['eventDateProviderColumn'] . '))') :
                            ('toDate(' . $options['eventDateProviderColumn'] . ')');
                } else {
                    throw new Exception('Column `' . $options['eventDateProviderColumn'] . '` with type `' . $columns[$options['eventDateProviderColumn']]['type']->getName() . '`, defined in `eventDateProviderColumn` option, has not valid DBAL Type');
                }
            }
            if (empty($options['eventDateColumn'])) {
                $dateColumns = array_filter($columns, function ($column) {
                    return $column['type'] instanceof DateType && $column['name'] !== 'EventDate';
                });

                if ($dateColumns) {
                    throw new Exception('Table `' . $tableName . '` has DateType columns: `' . implode('`, `',
                            array_keys($dateColumns)) . '`, but no one of them is setted as `eventDateColumn` with $table->addOption("eventDateColumn", "%eventDateColumnName%")');
                }

                $eventDateColumnName = 'EventDate';
            } else {
                if (isset($columns[$options['eventDateColumn']])) {
                    if ($columns[$options['eventDateColumn']]['type'] instanceof DateType) {
                        $eventDateColumnName = $options['eventDateColumn'];
                        unset($columns[$options['eventDateColumn']]);
                    } else {
                        throw new Exception('In table `' . $tableName . '` you have set field `' . $options['eventDateColumn'] . '` (' . get_class($columns[$options['eventDateColumn']]['type']) . ') as `eventDateColumn`, but it is not instance of DateType');
                    }
                } else {
                    $eventDateColumnName = $options['eventDateColumn'];
                }
            }
            $dateColumnParams['name'] = $eventDateColumnName;
            $columns = [$eventDateColumnName => $dateColumnParams] + $columns; // insert into very beginning

            /**
             * Primary key section
             */
            if (empty($options['primary'])) {
                //TODO: !!!!!!!!!!replace with primary key from ORM annotation
                $options['primary'] = ['version'];
                //throw new \Exception('You need specify PrimaryKey for MergeTree* tables');
            }

            $engineOptions = '(' . $eventDateColumnName . ', (' . implode(', ',
                    array_unique(array_values($options['primary']))) . '), ' . $indexGranularity;

            /**
             * any specific MergeTree* table parameters
             */
            if ('ReplacingMergeTree' === $engine) {
                if (!empty($options['versionColumn'])) {
                    if (!isset($columns[$options['versionColumn']])) {
                        throw new Exception('If you specify `versionColumn` for ReplacingMergeTree table -- you must add this column manually (any of UInt*, Date or DateTime types)');
                    }

                    if (
                        !$columns[$options['versionColumn']]['type'] instanceof IntegerType &&
                        !$columns[$options['versionColumn']]['type'] instanceof BigIntType &&
                        !$columns[$options['versionColumn']]['type'] instanceof SmallIntType &&
                        !$columns[$options['versionColumn']]['type'] instanceof DateType &&
                        !$columns[$options['versionColumn']]['type'] instanceof DateTimeType
                    ) {
                        throw new Exception('For ReplacingMergeTree tables `versionColumn` must be any of UInt* family, or Date, or DateTime types. ' . get_class($columns[$options['versionColumn']]['type']) . ' given.');
                    }

                    $engineOptions .= ', ' . $columns[$options['versionColumn']]['name'];
                }
            }

            $engineOptions .= ')';
        }

        $columnListSql = $this->getColumnDeclarationListSQL($columns);
        $query = 'CREATE TABLE ' . $tableName . ' (' . $columnListSql . ') ENGINE = ' . $engine . $engineOptions;

        $sql[] = $query;

        return $sql;
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getCreateForeignKeySQL(
        /** @noinspection PhpUnusedParameterInspection */
        ForeignKeyConstraint $foreignKey,
        $table
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getAlterTableSQL(
        /** @noinspection PhpUnusedParameterInspection */
        TableDiff $diff
    ) {
        $columnSql = [];
        $queryParts = [];
        if ($diff->newName !== false) {
            throw DBALException::notSupported('RENAME COLUMN');
        }

        foreach ($diff->addedColumns as $column) {
            if ($this->onSchemaAlterTableAddColumn($column, $diff, $columnSql)) {
                continue;
            }

            $columnArray = $column->toArray();
            $queryParts[] = 'ADD COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this), $columnArray);
        }

        foreach ($diff->removedColumns as $column) {
            if ($this->onSchemaAlterTableRemoveColumn($column, $diff, $columnSql)) {
                continue;
            }

            $queryParts[] = 'DROP COLUMN ' . $column->getQuotedName($this);
        }

        foreach ($diff->changedColumns as $columnDiff) {
            if ($this->onSchemaAlterTableChangeColumn($columnDiff, $diff, $columnSql)) {
                continue;
            }

            /* @var $columnDiff ColumnDiff */
            $column = $columnDiff->column;
            $columnArray = $column->toArray();

            // Don't propagate default value changes for unsupported column types.
            if ($columnDiff->hasChanged('default') &&
                count($columnDiff->changedProperties) === 1 &&
                ($columnArray['type'] instanceof TextType || $columnArray['type'] instanceof BlobType)
            ) {
                continue;
            }

            $queryParts[] = 'MODIFY COLUMN ' . $this->getColumnDeclarationSQL($column->getQuotedName($this),
                    $columnArray);
        }

        foreach ($diff->renamedColumns as $oldColumnName => $column) {
            throw DBALException::notSupported('RENAME COLUMN');
        }

        $sql = [];
        $tableSql = [];

        if (!$this->onSchemaAlterTable($diff, $tableSql)) {
            if (count($queryParts) > 0) {
                $sql[] = 'ALTER TABLE ' . $diff->getName($this)->getQuotedName($this) . ' ' . implode(', ',
                        $queryParts);
            }
        }

        return array_merge($sql, $tableSql, $columnSql);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    protected function getPreAlterTableIndexForeignKeySQL(
        /** @noinspection PhpUnusedParameterInspection */
        TableDiff $diff
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    protected function getPostAlterTableIndexForeignKeySQL(
        /** @noinspection PhpUnusedParameterInspection */
        TableDiff $diff
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    protected function getRenameIndexSQL(
        /** @noinspection PhpUnusedParameterInspection */
        $oldIndexName,
        Index $index,
        $tableName
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    protected function _getAlterTableIndexForeignKeySQL(
        /** @noinspection PhpUnusedParameterInspection */
        TableDiff $diff
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getColumnDeclarationSQL($name, array $field)
    {
        if (isset($field['columnDefinition'])) {
            $columnDef = $this->getCustomTypeDeclarationSQL($field);
        } else {
            $default = $this->getDefaultValueDeclarationSQL($field);

            $typeDecl = $field['type']->getSqlDeclaration($field, $this);
            $columnDef = $typeDecl . $default;
        }

        return $name . ' ' . $columnDef;
    }

    /**
     * {@inheritDoc}
     */
    public function getDecimalTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $columnDef
    ) {
        return 'String';
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getCheckDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $definition
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getUniqueConstraintDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        $name,
        Index $index
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getIndexDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        $name,
        Index $index
    ) {
        // Index declaration in statements like CREATE TABLE is not supported.
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getForeignKeyDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        ForeignKeyConstraint $foreignKey
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getAdvancedForeignKeyOptionsSQL(
        /** @noinspection PhpUnusedParameterInspection */
        ForeignKeyConstraint $foreignKey
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getForeignKeyReferentialActionSQL(
        /** @noinspection PhpUnusedParameterInspection */
        $action
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getForeignKeyBaseDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        ForeignKeyConstraint $foreignKey
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getUniqueFieldDeclarationSQL()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentDateSQL()
    {
        return 'today()';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimeSQL()
    {
        //TODO check it! time with 1970 year prefix...
        return 'toTime(now())';
    }

    /**
     * {@inheritDoc}
     */
    public function getCurrentTimestampSQL()
    {
        return 'now()';
    }

    /**
     * {@inheritDoc}
     */
    public function getListDatabasesSQL()
    {
        return 'SHOW DATABASES FORMAT JSON';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTableColumnsSQL($table, $database = null)
    {
        return 'DESCRIBE TABLE ' . ($database ? $this->quoteSingleIdentifier($database) . '.' : '') . $this->quoteSingleIdentifier($table) . ' FORMAT JSON';
    }

    /**
     * {@inheritDoc}
     */
    public function getListTablesSQL()
    {
        return "SELECT database, name FROM system.tables WHERE database != 'system' AND engine != 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getListViewsSQL($database)
    {
        return "SELECT name FROM system.tables WHERE database != 'system' AND engine = 'View'";
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateViewSQL($name, $sql)
    {
        return 'CREATE VIEW ' . $this->quoteIdentifier($name) . ' AS ' . $sql;
    }

    /**
     * {@inheritDoc}
     */
    public function getDropViewSQL($name)
    {
        return 'DROP TABLE ' . $this->quoteIdentifier($name);
    }

    /**
     * {@inheritDoc}
     */
    public function getCreateDatabaseSQL($database)
    {
        return 'CREATE DATABASE ' . $this->quoteIdentifier($database);
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $fieldDeclaration
    ) {
        return 'DateTime';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTimeTzTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $fieldDeclaration
    ) {
        return 'DateTime';
    }

    public function getTimeTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $fieldDeclaration
    ) {
        return 'String';
    }

    /**
     * {@inheritDoc}
     */
    public function getDateTypeDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $fieldDeclaration
    ) {
        return 'Date';
    }

    /**
     * {@inheritDoc}
     */
    public function getFloatDeclarationSQL(
        /** @noinspection PhpUnusedParameterInspection */
        array $fieldDeclaration
    ) {
        return 'Float64';
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getDefaultTransactionIsolationLevel()
    {
        throw DBALException::notSupported(__METHOD__);
    }

    /* supports*() methods */

    /**
     * {@inheritDoc}
     */
    public function supportsTransactions()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsSavepoints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsPrimaryConstraints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsForeignKeyConstraints()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public function supportsGettingAffectedRows()
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    protected function doModifyLimitQuery($query, $limit, $offset)
    {
        if (is_null($limit)) {
            return $query;
        }

        $query .= ' LIMIT ';
        if (!is_null($offset)) {
            $query .= $offset . ', ';
        }

        $query .= $limit;

        return $query;
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getEmptyIdentityInsertSQL(
        /** @noinspection PhpUnusedParameterInspection */
        $tableName,
        $identifierColumnName
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getTruncateTableSQL(
        /** @noinspection PhpUnusedParameterInspection */
        $tableName,
        $cascade = false
    ) {
        /**
         * For MergeTree* engines may be done with next workaround:
         *
         * SELECT partition FROM system.parts WHERE table= '$tableName';
         * ALTER TABLE $tableName DROP PARTITION $partitionName
         */
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function createSavePoint(
        /** @noinspection PhpUnusedParameterInspection */
        $savepoint
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function releaseSavePoint(
        /** @noinspection PhpUnusedParameterInspection */
        $savepoint
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function rollbackSavePoint(
        /** @noinspection PhpUnusedParameterInspection */
        $savepoint
    ) {
        throw DBALException::notSupported(__METHOD__);
    }

    /**
     * {@inheritDoc}
     */
    protected function getReservedKeywordsClass()
    {
        return ClickHouseKeywords::class;
    }

    /**
     * {@inheritDoc}
     * @throws DBALException
     */
    public function getDefaultValueDeclarationSQL($field)
    {
        if (!isset($field['default'])) {
            return '';
        }

        $default = " DEFAULT '" . $field['default'] . "'";
        if (isset($field['type'])) {
            if (in_array((string)$field['type'], [
                    'Integer',
                    'SmallInt',
                    'Float'
                ]) || ('BigInt' === $field['type'] && PDO::PARAM_INT === Type::getType('BigInt')->getBindingType())) {
                $default = ' DEFAULT ' . $field['default'];
            } else {
                if (in_array((string)$field['type'],
                        ['DateTime']) && $field['default'] == $this->getCurrentTimestampSQL()) {
                    $default = ' DEFAULT ' . $this->getCurrentTimestampSQL();
                } else {
                    if ('Date' == (string)$field['type'] || (string)$field['type'] === '\\Date') { // TODO check if string matches constant date like 'dddd-yy-mm' and quote it
                        $default = ' DEFAULT ' . $field['default'];
                    }
                }
            }
        }

        return $default;
    }

    /**
     * {@inheritDoc}
     */
    public function getDoctrineTypeMapping($dbType)
    {
        // FixedString
        if (strpos(strtolower($dbType), 'fixedstring') === 0) {
            $dbType = 'fixedstring';
        }

        //Enum8
        if (strpos(strtolower($dbType), 'enum8') === 0) {
            $dbType = 'enum8';
        }

        //Enum16
        if (strpos(strtolower($dbType), 'enum16') === 0) {
            $dbType = 'enum16';
        }
        return parent::getDoctrineTypeMapping($dbType);
    }

    /**
     * {@inheritDoc}
     */
    public function quoteStringLiteral($str)
    {
        $c = $this->getStringLiteralQuoteCharacter();

        return $c . addslashes($str) . $c;
    }

    /**
     * {@inheritDoc}
     */
    public function quoteSingleIdentifier($str)
    {
        $c = $this->getIdentifierQuoteCharacter();

        return $c . addslashes($str) . $c;
    }

}

<?php

namespace App\Services;

use DateTime;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;
use Throwable;

/**
 * Сервис для бекапа базы данных при помощи sql запросов
*/
class BackupDataBaseService
{
    /**массив данных по таблицам. Структура:
     * $arrayTables[$tableName]["created"] => true -флаг сделана запись создание таблицы,
     * $arrayTables[$tableName]["processed"] => true - все данные по таблицыданные внесены в скрипт sql,
     * $arrayTables[$tableName]["countRows"] => $countRows - количество записей в таблице,
     * $arrayTables[$tableName]["countRowsProcessed"] => 0 - количество строк внесенных в скрипт sql;
    */
    private $arrayTables;
    //текст скрипта sql
    private $output;
    //время начало работы скрипта / время работы после редиректа на самого себя
    private $timeStartRestore;
    //имя файла выгрузки sql из стороджа
    private $fileName;
    /**
     * Анализ таблиц в базе данных,
     * занесение их в структуру,
     * регистрация кол-ва в них строк
     * формирование директив обработки запросов
    */
    public function analysisTables()
    {
        $queryTables = 'SHOW TABLES';
        $this->arrayTables = [];
        $tablesData = DB::select($queryTables);
        foreach ($tablesData as $tableData) {
            $tableDataArray = (array)$tableData;
            $tableName = $tableDataArray[key($tableDataArray)];
            $queryCountRows = "SELECT count(1) as count FROM `$tableName` ";
            $countRows = DB::select($queryCountRows)[0]->count;
            if (!$countRows) {
                $countRows = 0;
            }
            $this->arrayTables[$tableName] = ["created" => false, "processed" => false,
                "countRows" => $countRows, "countRowsProcessed" => 0];
        }
        $this->output = "-- database backup - " . date('Y-m-d H:i:s') . PHP_EOL;
        $this->output .= "SET NAMES utf8;" . PHP_EOL;
        $this->output .= "SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';" . PHP_EOL;
        $this->output .= "SET foreign_key_checks = 0;" . PHP_EOL;
        $this->output .= "SET AUTOCOMMIT = 0;" . PHP_EOL;
        $this->output .= "START TRANSACTION;" . PHP_EOL;
        $this->timeStartRestore = new DateTime();

    }
    /**
     * единовременная блокировка таблиц с режимом READ
     * возвращает true при успешной блокировке
     * регистрация ошибки, если блокировка не доступна
    */
    public function lockTables()
    {
        $queryLock = "LOCK TABLES ";
        $i = 0;
        foreach ($this->arrayTables as $tableName => $table) {
            $queryLock .= $tableName . " READ";
            if ($i < (count($this->arrayTables) - 1))
                $queryLock .= ', ';
            $i++;
        }
        $queryLock .= ";";
        try {
            DB::connection()->getPdo()->exec($queryLock);
            return true;
        } catch (Throwable $e) {
            report($e);
            return false;
        }

    }
    /**
     * добавление записи в sql скрипт удаления таблицы $tableName , и ее создание
     * регистрация в структуре о том, что есть запись о создании таблицы
    */
    public function createTable($tableName)
    {
        $this->output .= "DROP TABLE IF EXISTS `$tableName`;" . PHP_EOL;
        $queryCreate = "SHOW CREATE TABLE `$tableName`";
        $createTableScript = DB::select($queryCreate);
        $createTableScriptArray = (array)$createTableScript[0];
        $this->output .= PHP_EOL . $createTableScriptArray["Create Table"] . ";" . PHP_EOL;
        $this->arrayTables[$tableName]["created"] = true;

    }
    /**
     * внесении части данных в скрипт по таблице $tableName,
     * кол-во строк ограничивается параметром $portion
     * берутся строки из таблицы начиная с уже обработанной части
    */
    public function processPortionTable($tableName, $portion)
    {
        $countRowsProcess = $this->arrayTables[$tableName]["countRowsProcessed"];
        $querySelect = "SELECT * FROM `$tableName` limit " . $countRowsProcess . ", " . $portion;
        $tableRows = DB::select("$querySelect");
        foreach ($tableRows as $tableRow) {
            $this->output .= "INSERT INTO `$tableName` VALUES(";
            $countFields = count((array)$tableRow);
            $i = 0;
            foreach ($tableRow as $field) {
                $fieldOut = addslashes($field);
                $fieldOut = str_replace("\n", "\\n", $fieldOut);
                if (isset($fieldOut))
                    $this->output .= "'" . $fieldOut . "'";
                else $this->output .= "''";
                if ($i < ($countFields - 1))
                    $this->output .= ',';
                $i++;
            }
            $this->output .= ");" . PHP_EOL;
        }

        if ($this->arrayTables[$tableName]["countRows"] > $countRowsProcess + $portion) {
            $this->arrayTables[$tableName]["countRowsProcessed"] = $countRowsProcess + $portion;
        } else {
            $this->arrayTables[$tableName]["countRowsProcessed"] = $this->arrayTables[$tableName]["countRows"];
            $this->arrayTables[$tableName]["processed"] = true;
        }
    }
    /**
     * Завершение коммита
     * разблокировка таблиц
     * выгрузка файла скрипта в сторедж
    */
    public function finish()
    {
        $this->output .= PHP_EOL . PHP_EOL;
        $this->output .= "COMMIT;";
        DB::connection()->getPdo()->exec("UNLOCK TABLES;");
        $fileName = "backup-" . date('Y_m_d-h_i_s') . '.sql';
        Storage::disk('local')->put($fileName, $this->output);
        $this->fileName = $fileName;
    }

    public function getFlagProcessedTable($tableName)
    {
        return $this->arrayTables[$tableName]["processed"];
    }
    /**
     * возвращает время работы от начала редиректа / запуска до текущего момента в секундах
    */
    public function getWorkingTime()
    {
        $currentTime = new DateTime();
        $difference = ($currentTime->diff($this->timeStartRestore))->s;
        return (int)$difference;
    }
    /**
     * возвращает имя файла в стородже
    */
    public function getFileName()
    {
        return $this->fileName;
    }
    /**
     * возвращает массив структцры таблиц
    */
    public function getTables()
    {
        return $this->arrayTables;
    }
    /**
     * Восстанваливает состояние структуры таблиц и скрипта sql из файлов
     * при установленном $debug = true файлы не удаляются
    */
    public function restoreFromFile($fileName)
    {
        $debug = false;
        $this->timeStartRestore = new DateTime();
        $contents = Storage::get($fileName);
        $this->arrayTables = json_decode($contents, true);
        $this->output = Storage::get($fileName."_output");
        if (!$debug){
            Storage::delete($fileName);
            Storage::delete($fileName."_output");
        }
        return true;
    }
    /**
     * Сохраняет состояние структуры таблиц и скрипта sql в файлы
    */
    public function saveToFile()
    {
        $fileNameBackupDB = 'backupDB' . time();
        Storage::disk('local')->put($fileNameBackupDB, json_encode($this->arrayTables));
        Storage::disk('local')->put($fileNameBackupDB."_output", $this->output);
        return $fileNameBackupDB;
    }


}

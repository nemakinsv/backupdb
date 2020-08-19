<?php

namespace App\Services;

use DateTime;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Storage;

class BackupDataBaseService
{
    //
    public $arrayTables;
    private $output;
    private $timeStartRestoreFromSession;
    public $fileName;

    public function analysisTables()
    {
        $queryTables = 'SHOW TABLES';
        $this->arrayTables = [];
        $tablesData = DB::select($queryTables);
        foreach ($tablesData as $tableData){
            $tableDataArray =  (array)$tableData;
            $tableName = $tableDataArray[key($tableDataArray)];
            $queryCountRows = "SELECT count(1) as count FROM `$tableName` ";
            $countRows = DB::select($queryCountRows)[0]->count;
            if (!$countRows){
                $countRows = 0;
            }
            $this->arrayTables[$tableName] = ["created"=> false, "processed"=> false,
                "countRows" =>$countRows, "countRowsProcessed" => 0];
        }
        $this->output = "-- database backup - ".date('Y-m-d H:i:s').PHP_EOL;
        $this->output .= "SET NAMES utf8;".PHP_EOL;
        $this->output .= "SET sql_mode = 'NO_AUTO_VALUE_ON_ZERO';".PHP_EOL;
        $this->output .= "SET foreign_key_checks = 0;".PHP_EOL;
        $this->output .= "SET AUTOCOMMIT = 0;".PHP_EOL;
        $this->output .= "START TRANSACTION;".PHP_EOL;
        $this->timeStartRestoreFromSession = new DateTime();

    }
    public function lockTables()
    {
        $queryLock = "LOCK TABLES ";
        $i = 0;
        foreach($this->arrayTables as $tableName => $table) {
            $queryLock.=$tableName." READ";
            if($i <(count($this->arrayTables)-1))
                $queryLock.= ', ';
            $i++;
        }
        $queryLock.=";";
        DB::connection()->getPdo()->exec($queryLock);
    }
    public function createTable($tableName)
    {
        $this->output .= "DROP TABLE IF EXISTS `$tableName`;".PHP_EOL;
        $queryCreate = "SHOW CREATE TABLE `$tableName`";
        $createTableScript = DB::select($queryCreate);
        $createTableScriptArray = (array)$createTableScript[0];
        $this->output .= PHP_EOL.$createTableScriptArray["Create Table"].";".PHP_EOL;
        $this->arrayTables[$tableName]["created"] = true;

    }
    public function processPortionTable($tableName, $portion)
    {
        $countRowsProcess = $this->arrayTables[$tableName]["countRowsProcessed"];
        $querySelect = "SELECT * FROM `$tableName` limit " . $countRowsProcess . ", " . $portion;
        $tableRows = DB::select("$querySelect");
        foreach($tableRows as $tableRow){
            $this->output .= "INSERT INTO `$tableName` VALUES(";
            $countFields =  count((array)$tableRow);
            $i = 0;
            foreach ($tableRow as $field){
                $fieldOut = addslashes($field);
                $fieldOut = str_replace("\n","\\n",$fieldOut);
                if (isset($fieldOut))
                    $this->output .= "'".$fieldOut."'";
                else $this->output .= "''";
                if ($i<($countFields-1))
                    $this->output .= ',';
                $i++;
            }
            $this->output .= ");".PHP_EOL;
        }

        if ($this->arrayTables[$tableName]["countRows"] > $countRowsProcess + $portion) {
            $this->arrayTables[$tableName]["countRowsProcessed"] = $countRowsProcess + $portion;
        }
        else {
            $this->arrayTables[$tableName]["countRowsProcessed"] = $this->arrayTables[$tableName]["countRows"];
            $this->arrayTables[$tableName]["processed"] = true;
        }
    }
    public function finish()
    {
        $this->output .= PHP_EOL.PHP_EOL;
        $this->output .= "COMMIT;";
        DB::connection()->getPdo()->exec("UNLOCK TABLES;");
        $fileName = "backup-".date('Y_m_d-h_i_s').'.sql';
        Storage::disk('local')->put($fileName, $this->output);
        $this->fileName = $fileName;
    }
    public function getFlagProcessedTable($tableName)
    {
        return $this->arrayTables[$tableName]["processed"];
    }

    public function getWorkingTime()
    {
        $currentTime = new DateTime();
        $difference = ($currentTime->diff($this->timeStartRestoreFromSession))->s;
        return (int)$difference;
    }
}

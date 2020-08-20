<?php

namespace App\Http\Controllers;

use App\Services\BackupDataBaseService;
use DateTime;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Storage;

class BackupDBController extends Controller
{
    /**
     * выполняет бекап базы данных, указанной в env
    */
    public function backup(Request $request)
    {
        $fileNameOldBackupDB = $request->get('fileNameBackupDB');
        if ($fileNameOldBackupDB) {
            $backupDB = new BackupDataBaseService();
            $backupDB->restoreFromFile($fileNameOldBackupDB);
        } else {
            $backupDB = new BackupDataBaseService();
            $backupDB->analysisTables();
            $result = $backupDB->lockTables();
            if(!$result)
                return view('output', ["output" => "блокировка не доступна"]);
        }
        foreach ($backupDB->getTables() as $tableName => $table) {
            if (!$backupDB->getFlagTableCreated($tableName)) {
                $backupDB->createTable($tableName);
            }
            while (!$backupDB->getFlagProcessedTable($tableName)) {
                $backupDB->processPortionTable($tableName, 1000);
                if ($backupDB->getWorkingTime() > 20) {
                    $fileNameBackupDB = $backupDB->saveToFile();
                    return redirect()->route('backupdb', ['fileNameBackupDB' => $fileNameBackupDB]);
                }
            }
        }
        $backupDB->finish();
        return Storage::download($backupDB->getFileName());
    }
}

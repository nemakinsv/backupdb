<?php

namespace App\Http\Controllers;

use App\Services\BackupDataBaseService;
use DateTime;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Storage;

class BackupDBController extends Controller
{

    public function backup(Request $request)
    {
        $sessionKeyOldBackupDB = $request->get('sessionKeyBackupDB');
        if (!$sessionKeyOldBackupDB) {
            $backupDB = new BackupDataBaseService();
            $backupDB->analysisTables();
            $backupDB->lockTables();
        }
        else {
            $backupDB = session($sessionKeyOldBackupDB);
            if (!$backupDB){
                return view('output', ["output" => "Ошибка, нет в сессии backupDB " . $sessionKeyOldBackupDB]);
            }
            $backupDB->timeStartRestoreFromSession = new DateTime();
        }
        foreach($backupDB->getTables() as $tableName => $table) {
            if(!$table["created"]){
                $backupDB->createTable($tableName);
            }
            while (!$backupDB->getFlagProcessedTable($tableName)){
                $backupDB->processPortionTable($tableName, 200);
                //sleep(2);
                if ($backupDB->getWorkingTime() > 10){
                    $sessionKeyBackupDB = 'backupDB'.time();
                    session([$sessionKeyBackupDB => $backupDB]);
                    if ($sessionKeyOldBackupDB) {
                        $request->session()->forget($sessionKeyOldBackupDB);
                    }
                    return redirect()->route('backupdb', ['sessionKeyBackupDB' => $sessionKeyBackupDB]);
                }
            }
        }
        $backupDB->finish();
        $request->session()->forget($sessionKeyOldBackupDB);
        return Storage::download($backupDB->getFileName());
    }
}

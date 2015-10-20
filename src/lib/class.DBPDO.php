<?php
/**
 * $Id$
 *
 * Mini mySQL class
 *
 * @modified 2015-04-23
 * @version 1.0
 *
 */
class slyr_SQL {

	public $error;

    public $pdo  =false;

	private $hostname;
	private $username;
	private $password;
	private $database;
    private $persistent = false;

    function __construct ($dbn, $usr, $pwd, $host = 'localhost') {

        $this->database = $dbn;
        $this->username = $usr;
        $this->password = $pwd;
        $this->hostname = $host;
        $this->connect();
    }

    function prep_query ($query){

        return $this->pdo->prepare($query);
    }

    function connect(){

        if (!$this->pdo) {
            try {
                $this->pdo=new PDO('mysql:dbname='.$this->database.';host='.$this->hostname, $this->username, $this->password,
                                   array(PDO::ATTR_PERSISTENT=>$this->persistent));
            } catch (PDOException $e) {
                $this->error = $e->getMessage();
                return false;
            }
        } else {
            $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_WARNING);
        }
        return true;
    }

    function execute ($query, $values = null) {

        if      ($values == null)    $values = array();
        else if (!is_array($values)) $values = array($values);
        $stmt = $this->prep_query($query);
        if ($stmt->execute($values) === true) {
            if      (stripos($query, 'insert ' ) === 0)          { $out = $this->pdo->lastInsertId();        }
            else if (preg_match('/^(select|show)\s+/i', $query)) { $out = $stmt->fetchAll(PDO::FETCH_ASSOC); }
            else                                                 { $out=true;                                }
            $this->error = '';
        } else {
            $out = null;
            $err=$stmt->errorInfo();
            $this->error = 'SQL error: ('.$err[1].') '.$err[2];
        }
        return $out;
    }

    function lastInsertId() {

        return $this->pdo->lastInsertId();
    }
}
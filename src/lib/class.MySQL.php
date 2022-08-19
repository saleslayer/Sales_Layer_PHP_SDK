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

    public  $error;
    public  $result;
    public  $records;
    private $hostname;
    private $username;
    private $password;
    private $database;
    private $dbLink;

    function __construct ($dbn, $usr, $pwd, $host='localhost', $port=3306) {

        $this->database = $dbn;
        $this->username = $usr;
        $this->password = $pwd;
        $this->hostname = $host.':'.$port;
        $this->Connect();
    }

    function __destruct(){

        $this->closeConnection();
    }

    private function Connect () {

        $this->CloseConnection();
        $this->dbLink = mysqli_connect ($this->hostname, $this->username, $this->password);
        if (!$this->dbLink) {
            $this->error = 'Could not connect to server: '.mysqli_error($this->dbLink);
            return false;
        }
        if ($this->database && !$this->useDB()) return false;
        return true;
    }

    private function useDB () {

        if(!mysqli_select_db($this->database, $this->dbLink)){
            $this->error = 'Cannot select database: '.mysqli_error($this->dbLink);
            return false;
        }
        return true;
    }

    public function execute ($query) {

        if ($this->result = mysqli_query($query, $this->dbLink)){
            $this->records  = (gettype($this->result) === 'resource' ? @mysqli_num_rows($this->result): 0);
            $this->affected = @mysqli_affected_rows($this->dbLink);
            $this->error    = null; 
            return $this->arrayResults();
        } else {
            $this->error = mysqli_error($this->dbLink);
            return false;
        }
    }

    public function arrayResults () {

        $list = array();
        if ($this->records) { while ($data = mysqli_fetch_assoc($this->result)) $list[] = $data; }
        return $list;
    }

    public function lastInsertID () {

        return mysqli_insert_id($this->dbLink);
    }
}
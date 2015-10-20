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

    function __construct ($dbn, $usr, $pwd, $host='localhost', $port=3306, $persist=false) {

        $this->database = $dbn;
        $this->username = $usr;
        $this->password = $pwd;
        $this->hostname = $host.':'.$port;
        $this->Connect($persist);
    }

    function __destruct(){

        $this->closeConnection();
    }

    private function Connect ($persist=false) {

        $this->CloseConnection();
        $this->dbLink =($persist ? mysql_pconnect($this->hostname, $this->username, $this->password)
                                   :
                                   mysql_connect ($this->hostname, $this->username, $this->password));
        if (!$this->dbLink) {
            $this->error = 'Could not connect to server: '.mysql_error($this->dbLink);
            return false;
        }
        if (!$this->useDB()) return false;
        return true;
    }

    private function useDB () {

        if(!mysql_select_db($this->database, $this->dbLink)){
            $this->error = 'Cannot select database: '.mysql_error($this->dbLink);
            return false;
        }
        return true;
    }

    public function execute ($query) {

        if ($this->result = mysql_query($query, $this->dbLink)){
            $this->records  = (gettype($this->result) === 'resource' ? @mysql_num_rows($this->result): 0);
            $this->affected = @mysql_affected_rows($this->dbLink);
            return $this->arrayResults();
        } else {
            $this->error = mysql_error($this->dbLink);
            return false;
        }
    }

    public function arrayResults () {

        $list = array();
        if ($this->records) { while ($data = mysql_fetch_assoc($this->result)) $list[] = $data; }
        return $list;
    }

    public function lastInsertID () {

        return mysql_insert_id($this->dbLink);
    }
}
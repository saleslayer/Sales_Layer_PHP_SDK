<?php
/**
 * $Id$
 *
 * Created by Iban Borras.
 *
 * CreativeCommons License Attribution (By):
 * http://creativecommons.org/licenses/by/4.0/
 *
 * SalesLayer Updater database class is a library for update and connection to Sales Layer API
 *
 * @modified 2016-03-21
 * @version 1.12
 *
 */

if                                (!class_exists('SalesLayer_Conn')) include_once dirname(__FILE__).DIRECTORY_SEPARATOR.'SalesLayer-Conn.php';
if (extension_loaded('PDO')) { if (!class_exists('slyr_SQL'))        include_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.DBPDO.php'; }
else if                           (!class_exists('slyr_SQL'))        include_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.MySQL.php';

class SalesLayer_Updater extends SalesLayer_Conn {

    public  $updater_version = '1.12';

    public  $database = null;
    public  $username = null;
    public  $password = null;
    public  $hostname = null;

    public  $table_prefix = 'slyr_';
    public  $table_config = '__api_config';
    public  $table_engine = 'InnoDB';

    public  $list_connectors = array();

    public  $DB       = null;
    public  $SQL_list = array();

    public  $debbug   = true; // <-- false / true / 'file'

    private $database_tables = null;
    private $database_fields = array();
    private $database_config = array();
    private $mysql_version   = null;

    private $database_field_types = array(

        'string'    =>'text',
        'big_string'=>'mediumtext',
        'numeric'   =>'double',
        'boolean'   =>'bool',
        'image'     =>'text',
        'file'      =>'text',
        'datetime'  =>'datetime',
        'list'      =>'text',
        'key'       =>'bigint'

    );

    private $database_field_types_charset = array(

        'text'      =>'CHARACTER SET {collation} NOT NULL',
        'mediumtext'=>'CHARACTER SET {collation} NOT NULL',
        'bool'      =>'NOT NULL',
        'float'     =>'NOT NULL',
        'datetime'  =>'NOT NULL',
        'int'       =>'NOT NULL'
    );

    /**
     * Constructor - if you're not using the classally
     *
     * @param string $database Database name
     * @param string $username Username
     * @param string $password Password
     * @param string $hostname Database host
     * @param string $codeConn Code Connector Identificator key
     * @param string $secretKey Secret key
     * @param boolean $SSL Enable SSL
     * @param string $url Url to SalesLayer API connection
     * @return void
     *
     */

    public function __construct ($database=null, $username=null, $password=null, $hostname=null, $codeConn=null, $secretKey=null, $SSL=false, $url=false) {

        if ($this->__has_system_requirements() && $database!=null) {

               $this->connect($database, $username, $password, $hostname, $codeConn, $secretKey, $SSL, $url);
        }
    }

    /**
     * Get Updater class version
     *
     * @return string
     *
     */

    public function get_updater_class_version () {

        return $this->updater_version;
    }

    /**
     * Connect to the database and API class
     *
     * @param string $database Database name
     * @param string $username Username
     * @param string $password Password
     * @param string $hostname Database host
     * @param string $codeConn Code Connector Identificator key
     * @param string $secretKey Secret key
     * @param boolean $SSL Enable SSL
     * @param string $url Url to SalesLayer API connection
     * @return void
     *
     */

    public function connect ($database=null, $username=null, $password=null, $hostname=null, $codeConn=null, $secretKey=null, $SSL=false, $url=false) {

        if (!$this->response_error) {

            $this->database_connect($database, $username, $password, $hostname);

            if (!$this->response_error) {

                $this->table_prefix = strtolower($this->table_prefix);
                $this->table_config = strtolower($this->table_config);

                if (!in_array($this->table_prefix.$this->table_config, $this->get_database_tables())) { $this->__initialize_database(); }

                parent::__construct($codeConn, $secretKey, $SSL, $url);

                $this->__get_config();
            }
        }
    }

    /**
     * Database connect
     *
     * @param string $database Database name
     * @param string $username Username
     * @param string $password Password
     * @param string $hostname Database host
     * @return boolean
     *
     */

    public function database_connect ($database=null, $username=null, $password=null, $hostname=null) {

        $this->__set_database_credentials ($database, $username, $password, $hostname);

        $this->DB = new slyr_SQL($this->database, $this->username, $this->password, $this->hostname);

        if ($this->DB->error!=null) {

            $this->__trigger_error ($this->DB->error, 104);

            return false;
        }

        $this->DB->execute("SET NAMES 'utf8';");

        return true;
    }

    /**
     * Set the prefix for our tables if need change
     *
     * @param string $prefix to the tables
     * @return void
     *
     */

    public function set_table_prefix ($prefix) {

        $this->table_prefix=$prefix;
    }

    /**
     * Set the database credentials
     *
     * @param string $database Database name
     * @param string $username Username
     * @param string $password Password
     * @param string $hostname Database host
     * @return void
     *
     */

    private function __set_database_credentials ($database=null, $username=null, $password=null, $hostname=null) {

        if ($database!=null) { $this->database = $database; }
        if ($database!=null) { $this->username = $username; }
        if ($database!=null) { $this->password = $password; }
        if ($database!=null) { $this->hostname = $hostname; }
    }

    /**
     * Test system requirements
     *
     * @return boolean
     *
     */

    private function __has_system_requirements () {

        if (!extension_loaded('mysql')) {

            $this->__trigger_error ('Missing PHP MySQL extension', 103);

            return false;

        } else if (!extension_loaded('CURL')) {

            $this->__trigger_error ('Missing CURL extension', 106);

            return false;
        }

        return true;
    }

    /**
     * Initialize essential database tables
     *
     * @return boolean
     *
     */

    private function __initialize_database () {

        if (!in_array($this->get_response_error(), array(103, 104))) {

            $config_table=$this->table_prefix.$this->table_config;
            $tables      =$this->get_database_tables();

            if (!in_array($config_table, $tables)) {

                $SQL="CREATE TABLE IF NOT EXISTS `$config_table` (".
                     '`cnf_id` int(11) NOT NULL AUTO_INCREMENT, '.
                     '`conn_code` varchar(32) NOT NULL, '.
                     '`conn_secret` varchar(32) NOT NULL, '.
                     '`comp_id` int(11) NOT NULL, '.
                     '`last_update` timestamp NOT NULL, '.
                     '`default_language` varchar(6) NOT NULL, '.
                     '`languages` varchar(512) NOT NULL, '.
                     '`conn_schema` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, '.
                     '`data_schema` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, '.
                     '`conn_extra` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, '.
                     '`updater_version` varchar(10) NOT NULL, '.
                     'PRIMARY KEY (`cnf_id`)'.
                     ') ENGINE='.$this->table_engine.' DEFAULT CHARSET=latin1 AUTO_INCREMENT=1';

                if ($this->DB->execute($this->SQL_list[] = $SQL)) {

                    $this->database_tables[] = $config_table;

                    return true;

                } else {

                    $this->__trigger_error($this->DB->error." ($SQL)", 104);
                }
            }
        }

        return false;
    }

    /**
     * Update database configurations from API response
     *
     * @return boolean
     *
     */

    private function __update_config ($update_last_upd=true) {

        if (!in_array($this->get_response_error(), array(103, 104)) && $code=addslashes($this->get_identification_code())) {

            if ($this->get_response_action() == 'refresh') {

                $info   = $this->get_response_table_information();
                $schema = array();

                if (is_array($info) and count($info)) {

                    foreach ($info as $table=>$data) {

                        if ($bd_table = strtolower($table)) {

                            if (!isset($schema[$bd_table]))  { $schema[$bd_table] = array(); }

                            if (isset($data['table_joins'])) { $schema[$bd_table]['table_joins'] = $data['table_joins']; }

                            $schema[$bd_table]['name'] = $table;

                            foreach ($data['fields'] as $field=>$struc) {

                                if ($field) {

                                    if (isset($struc['has_multilingual']) && $struc['has_multilingual']) {

                                        $db_field=strtolower($struc['basename']);

                                        if (!isset($schema[$bd_table][$db_field])) {

                                            $schema[$bd_table]['fields'][$db_field]=array(

                                                'name'            =>$struc['basename'],
                                                'type'            =>$struc['type'],
                                                'has_multilingual'=>1
                                            );

                                            if ($struc['type']=='image') {

                                                $schema[$bd_table]['fields'][$db_field]['image_sizes']=$struc['image_sizes'];
                                            }
                                        }

                                    } else {

                                        $db_field=strtolower($field);

                                        $schema[$bd_table]['fields'][$db_field]        =$struc;
                                        $schema[$bd_table]['fields'][$db_field]['name']=$field;
                                    }
                                }
                            }
                        }
                    }
                }

                unset($info, $data);
            }

            $this->get_connectors_list();

            $mode=((!isset($this->list_connectors['names']) || in_array($code, $this->list_connectors['names']))  ? 'update' : 'insert');

            $SQL="$mode `".                  $this->table_prefix.$this->table_config."` set ".
                 "`conn_code` = '".          $code                                                           ."', ".
                 "`conn_secret` = '".        addslashes($this->get_identification_secret())                  ."', ".
                 "`comp_id` = '".            addslashes($this->get_response_company_ID()                    )."', ".
                 ($update_last_upd ?
                    "`last_update` = '"     .addslashes($this->get_response_time()).                          "', " : '').
                 (($this->get_response_action() == 'refresh') ?
                    "`default_language` = '".addslashes($this->get_response_default_language()              )."', ".
                    "`languages` = '".       addslashes(implode(',', $this->get_response_languages_used())  )."', ".
                    "`conn_schema` = '".     addslashes(json_encode( $this->get_response_connector_schema()))."', ".
                    "`data_schema` = '".     addslashes(json_encode( $schema)                               )."', "
                    :
                    ''
                 ).
                 "`updater_version` = '".    addslashes($this->get_response_api_version()                   )."' ".
                 ($mode=='update' ? "where `conn_code`='$code' limit 1" : '');

            if ($this->DB->execute($this->SQL_list[] = $SQL)) {

                if ($mode=='insert') { $this->get_connectors_list($code); }

                $this->__get_config('', true);

                 return true;
            }

            $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Set last updated connector
     *
     * @return bool
     */
     
    private function __refresh_last_update_config () {
        
        if ($this->get_response_time() && $code=addslashes($this->get_identification_code())) {
            
            $SQL = "update `".$this->table_prefix.$this->table_config."` set last_update='".addslashes($this->get_response_time())."' where conn_code='$code' limit 1";
            
            if ($this->DB->execute($this->SQL_list[] = $SQL)) return true;
            
            $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }
        
        return false;
    }

    /**
     * Set connector credentials
     *
     */

    public function set_identification ($codeConn, $secretKey=null) {

        if (isset($this->database_config['conn_code']) && $codeConn!=$this->database_config['conn_code']) {

            $this->database_config=array();
        }

        parent::set_identification($codeConn, $secretKey);
    }

    /**
     * Get configured connector codes
     *
     * @return array
     *
     */

    public function get_connectors_list ($code=null) {

        if (!isset($this->list_connectors['names']) || !count($this->list_connectors['names'])) {

            $this->list_connectors['names']=array();

            $list=$this->DB->execute($this->SQL_list[]='select `conn_code` from `'.$this->table_prefix.$this->table_config.'`');

            if (count($list)) {

                foreach ($list as $v) { $this->list_connectors['names'][]=$v['conn_code']; }
            }
        }

        if ($code && (!count($this->list_connectors['names']) || !in_array($code, $this->list_connectors['names']))) {

            $this->list_connectors['names'][]=$code; $this->get_connectors_info($code);
        }

        return $this->list_connectors['names'];
    }

    /**
     * Get configured connector data
     *
     * @param string $code for get only data from specified connector
     * @return array
     *
     */

    public function get_connectors_info ($code=null, $refresh_info = false) {

        if ($refresh_info){ unset($this->list_connectors['data']); }

        if (!isset($this->list_connectors['data']) || !count($this->list_connectors['data']) || ($code && !isset($this->list_connectors['data'][$code]))) {

            $list=$this->DB->execute($this->SQL_list[]='select * from `'.$this->table_prefix.$this->table_config.'`'.
                                                        (isset($this->list_connectors['data'][$code]) ? ' where `conn_code`=\''.addslashes($code).'\' limit 1' : ''));

            if (count($list)) {

                if (!$code or !isset($this->list_connectors['data'])) $this->list_connectors['data']=array();

                foreach ($list as &$v) {

                    foreach ($v as &$w) { if (substr($w, 0, 1)=='{') { $w=json_decode($w, 1); }} unset($w);

                    $this->list_connectors['data'][$v['conn_code']]=$v;
                }

                unset($v, $w);
            }
        }

        return ($code ? (isset($this->list_connectors['data'][$code]) ? $this->list_connectors['data'][$code] : array())
                        :
                        $this->list_connectors['data']);
    }

    /**
     * Get extra info from connector
     *
     * @param string $code connector
     * @return array
     *
     */

    public function get_connector_extra_info ($code) {

        $SQL='select `conn_extra` from `'.$this->table_prefix.$this->table_config.'` where `conn_code`=\''.addslashes($code).'\' limit 1';

        if ($res=$this->DB->execute($this->SQL_list[] = $SQL)) {

            return json_decode($res[0]['conn_extra'], 1);
        }

        $this->__trigger_error($this->DB->error." ($SQL)", 104);

        return array();
    }

    /**
     * Set extra info into connector
     *
     * @param string $code connector
     * @param array $data to save
     * @param boolean $refresh for clean existing data
     * @return boolean
     *
     */

    public function set_connector_extra_info ($code, $data, $refresh=false) {

        if (is_array($data)) {

            if (!$refresh) {

                $now=$this->get_connector_extra_info($code);

                $data=array_merge((array)$now, $data);
            }

            $SQL='update `'.$this->table_prefix.$this->table_config.'` set `conn_extra`=\''.json_encode($data).
                 '\' where `conn_code`=\''.addslashes($code).'\' limit 1';

            if ($this->DB->execute($this->SQL_list[] = $SQL)) { return true; }

            $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Get database configurations
     *
     * @return array
     *
     */

    private function __get_config ($code='', $refresh=false) {

        if (!in_array($this->get_response_error(), array(103, 104)) && $this->get_connectors_list()) {

            if (!$code) { $code=addslashes($this->get_identification_code()); }

            if ( $code) {

                if ($refresh || !count($this->database_config) || $this->database_config['conn_code']!=$code) {

                    $data=$this->DB->execute($this->SQL_list[]='select * from `'.$this->table_prefix.$this->table_config."` where `conn_code`='$code' limit 1");

                    if (count($data)) {

                        $config=array(

                            'conn_id'         =>             $data['0']['cnf_id'],
                            'conn_code'       =>             $data['0']['conn_code'],
                            'comp_id'         =>             $data['0']['comp_id'],
                            'last_update'     =>             $data['0']['last_update'],
                            'default_language'=>             $data['0']['default_language'],
                            'languages'       =>explode(',', $data['0']['languages']),
                            'conn_schema'     =>json_decode( $data['0']['conn_schema'], 1),
                            'data_schema'     =>json_decode( $data['0']['data_schema'], 1)
                        );

                        if ( $config['last_update']=='0000-00-00 00:00') { $config['last_update']=null; }
                    }

                } else { return $this->database_config; }
            }
        }
        
        $config = isset($config) ? $config : null;
        
        return $this->database_config = $config;
    }

    /**
     * Construct the real field name
     *
     * @return string
     */

    private function __get_real_field ($field, $table, $language=null) {

        $table =strtolower($table);
        $schema=$this->get_database_table_schema($table, false);
        $fields=$this->get_database_table_fields($table);

        if (is_array($schema) && (isset($schema[$field]) || isset($fields[$field]))) {

            $field=$field.((isset($schema[$field]) && $schema[$field]['has_multilingual']) ? '_'.$this->__test_language($language) : '');

            if (isset($fields[$field])) { return $field; }
        }

        return '';
    }

    /**
     * Test if language code exist in database
     *
     * @return string
     */

    private function __test_language ($language) {

        $languages=$this->get_languages();

        if (!is_string($language)) {

            $language=$this->get_default_language();

            if (!in_array($language, $languages)) { $language=reset($languages); }

        } else {

            if (!in_array($language, $languages)) { $language=($default=$this->get_default_language() ? $default : reset($languages)); }
        }

        return $language;
    }

    /**
     * Get MySQL verision
     *
     * @return float number
     */

    private function __get_mysql_version () {

        if ($this->mysql_version) {

            $SQL='SHOW VARIABLES LIKE "%version%"';

            if (!($res=$this->DB->execute($this->SQL_list[] = $SQL))) {

                $this->__trigger_error($this->DB->error." ($SQL)", 104);

            } else {

                $list=explode('.', $res['version']);
                $ver =array_shift($list);

                if (count($list)) { $ver.='.'; foreach ($list as $l) { $ver.=sprintf('%02s', $l); }}

                $this->mysql_version=floatval($ver);
            }
        }

        return $this->mysql_version;
    }

    /**
     * Define utf mode
     *
     * @return string
     */

    private function __identifies_utf_mode () {

        $ver = $this->__get_mysql_version();

        return (($ver===null or $ver<5.0503) ? 'utf8 COLLATE utf8_general_ci' : 'utf8mb4 COLLATE utf8mb4_unicode_ci');
    }

    /**
     * Fix SQL collation
     *
     * @return string
     */

    private function __fix_collation ($sql) {

        return str_replace('{collation}', $this->__identifies_utf_mode(), $sql);
    }

    /**
     * Get table internal name
     *
     * @param $table string database table
     * @return name
     *
     */

    public function get_database_table_name ($table) {

        if (!$this->database_config) $this->__get_config();

        $table=strtolower($table);

        return (isset($this->database_config['data_schema'][$table]['name']) ? $this->database_config['data_schema'][$table]['name'] : $table);
    }

    /**
     * Get table schema
     *
     * @param $table string database table
     * @param $extended boolean extends multilingual fields
     * @return array
     *
     */

    public function get_database_table_schema ($table, $extended=true) {

        if (!$this->database_config) $this->__get_config();

        $table=strtolower($table);

        if (isset($this->database_config['data_schema'][$table]['fields'])) {

            $join_fields=array();

            if (    isset($this->database_config['data_schema'][$table]['table_joins'])) {

                 foreach ($this->database_config['data_schema'][$table]['table_joins'] as $field_id=>$table_rel) {

                     $join_fields[strtolower($field_id)]=array(

                        'type' =>'key',
                        'table'=>$table_rel,
                        'name' =>$field_id
                    );
                }
            }

            if ($extended!=true) {

                $fields=array_merge($join_fields, $this->database_config['data_schema'][$table]['fields']);

            } else {

                $fields=array(); $languages=$this->get_languages();

                foreach ($this->database_config['data_schema'][$table]['fields'] as $field=>$info) {

                    if (isset($info['has_multilingual']) && $info['has_multilingual']) {

                        foreach ($languages as $lang) {

                            $lfield=$field.'_'.$lang;

                            $fields[$lfield]            =$info;
                            $fields[$lfield]['language']=$lang;
                            $fields[$lfield]['basename']=$field;
                        }

                    } else {

                        $fields[$field]=$info;
                    }
                }

                $fields=array_merge($join_fields, $fields);
            }

            return $fields;
        }

        return false;
    }

    /**
     * Get table joins
     *
     * @return array
     *
     */

    public function get_database_table_joins ($table) {

        $table=strtolower($table);

        return (isset($this->database_config['data_schema'][$table]['table_joins'])) ? $this->database_config['data_schema'][$table]['table_joins'] : false;
    }

    /**
     * Update database
     *
     * @param $params array parameters
     * @return string
     *
     */

    public function update ($params=null, $connector_type=null, $force_refresh=false) {

        if ($code=$this->get_identification_code()) {

            if (!$this->database_config) $this->__get_config($code);

            if ($force_refresh==true ||
                (isset($this->database_config['conn_code']) && $this->get_identification_code()!=$this->database_config['conn_code'])) {

                $this->database_config['last_update']=null;
            }

            $this->get_info($this->database_config['last_update'], $params, $connector_type);

            if (!$this->has_response_error()) {

                $this->__update_config(false);

                if ($force_refresh==true) { $this->delete_all(false); }

                $this->get_database_tables();

                $tables=array_keys($this->get_response_table_information());

                foreach ($tables as $table) {

                    $table=strtolower($table);

                    if (!in_array($this->table_prefix.$table, $this->database_tables)) {

                        $this->create_database_table($table);

                    } else {

                        $this->update_database_table($table);
                    }
                }

                if (count($this->get_response_table_modified_ids()) || count($this->get_response_table_deleted_ids())) {

                    foreach ($tables as $table) {

                        $this->update_database_table_data($table);
                    }
                }

                $this->__refresh_last_update_config();

                return true;
            }

        } else {

            $this->__trigger_error('Invalid connector code', 2);
        }

        return false;
    }

    /**
     * Get database tables
     *
     * @param string $refresh list tables
     * @return array
     */

    public function get_database_tables ($refresh=false) {

        if ($this->database_tables === null || $refresh == true) {

            $tables=$this->DB->execute($this->SQL_list[]='SHOW TABLES');

            $this->database_tables=array();

            if (is_array($tables) && count($tables)) {

                foreach ($tables as $v) { $this->database_tables[]=((is_array($v)) ? reset($v) : $v); }
            }
        }

        return $this->database_tables;
    }

    /**
     * Get database table fields
     *
     * @param string $table table name
     * @return array
     */

    public function get_database_table_fields ($table, $refresh=false) {

        $table=strtolower($table);

        if (!isset($this->database_fields[$table]) || $refresh==true) {

            $this->database_fields[$table]=array();

            $data=$this->DB->execute($this->SQL_list[]='SHOW COLUMNS FROM `'.$this->table_prefix.$table.'`');

            if (is_array($data) && count($data)) {

                foreach ($data as $v) { $this->database_fields[$table][$v['Field']]=preg_replace('/^([^\s\(]+).*$/', '\\1', $v['Type']); }
            }
        }

        return $this->database_fields[$table];
    }

    /**
     * Get database last update
     *
     * @return string
     */

    public function get_database_last_update () {

        return (isset($this->database_config['last_update']) ? $this->database_config['last_update'] : null);
    }

    /**
     * Get database connector codename
     *
     * @return string
     */

    public function get_database_connector_code () {

        return (isset($this->database_config['conn_code']) ? $this->database_config['conn_code'] : null);
    }

    /**
     * Test if table exist in the database
     *
     * @param string $table table name
     * @return boolean
     */

    public function has_database_table ($table) {

        $table=strtolower($table);

        return (isset($this->database_tables[$this->table_prefix.$table]) ? true : false);
    }

    /**
     * Returs database type from pseudo type schema
     *
     * @param $type string type
     * @return string
     */

    private function __get_database_type_schema ($type) {

        return (isset($this->database_field_types[$type]) ? $this->database_field_types[$type] : $this->database_field_types['string']);
    }

    /**
     * Create database table
     *
     * @param $table string table name
     * @return boolean
     */

    public function create_database_table ($table) {

        $table = strtolower($table); $this->get_database_tables(true);

        if (!in_array($this->table_prefix.$table, $this->database_tables)) {

            $schema = $this->get_database_table_schema($table);

            if (is_array($schema) and count($schema)) {

                $fields="`{$table}_id` int not null auto_increment primary key, `__conn_id__` varchar(512) NOT NULL";

                foreach ($schema as $field=>$info) {

                    $field=strtolower($field);
                    $type =$this->__get_database_type_schema($info['type']);

                    $fields.=", `$field` $type ".$this->database_field_types_charset[$type];
                }

                $sly_table=$this->table_prefix.$table;

                $this->DB->execute($this->SQL_list[]="DROP TABLE IF EXISTS `$sly_table`");

                $SQL=$this->__fix_collation("CREATE TABLE `$sly_table` ($fields) ENGINE=".$this->table_engine.' DEFAULT CHARSET=latin1 AUTO_INCREMENT=1');

                if ($this->DB->execute($this->SQL_list[] = $SQL)) {

                    $this->database_tables[]=$sly_table;

                    unset($this->database_fields[$table]);

                    return true;

                } else {

                    $this->__trigger_error($this->DB->error." ($SQL)", 104);
                }
            }

        } else {

            return $this->update_database_table($table);
        }

        return false;
    }

    /**
     * Update database table
     *
     * @param $table string table name
     * @return boolean
     */

// ALTER TABLE `catalogue` ENGINE = InnoDB;

    public function update_database_table ($table) {

        $table=strtolower($table); $this->get_database_tables(true);

        if (in_array($this->table_prefix.$table, $this->database_tables)) {

            if ($this->get_response_action()=='refresh') {

                $SQL=(count($this->get_connectors_list())==1 ?

                        "TRUNCATE `".   $this->table_prefix.$table."`;"
                        :
                        "DELETE FROM `".$this->table_prefix.$table."` WHERE fin_in_set('".$this->database_config['conn_id']."', `__conn_id__`);");

                $this->DB->execute($this->SQL_list[] = $SQL);
            }

            $schema=$this->get_database_table_schema($table);

            if (is_array($schema) && count($schema)) {

                $this->get_database_table_fields($table, true);

                $fields='';

                foreach ($schema as $field=>$info) {

                    if ($info) {

                        $type=$this->__get_database_type_schema($info['type']);

                        $mode=((  !isset($this->database_fields[$table][$field])) ?

                                'ADD' : ($this->database_fields[$table][$field]!=$type ? "CHANGE `$field` " : ''));

                        if ($mode) {

                            $fields.=(($fields) ? ', ' : '')."$mode `$field` $type ".($type=='bigint' ? ' UNSIGNED' : '').
                                     $this->database_field_types_charset[$type];
                        }
                    }
                }

                if (count($this->get_connectors_list())==1) {

                    foreach (array_keys($this->database_fields[$table]) as $field) {

                        if ($field!=$table.'_id' && $field!='__conn_id__' && !isset($schema[$field])) {

                            $fields.=(($fields) ? ', ' : '')."DROP `$field`";
                        }
                    }
                }

                if ($fields) {

                    $SQL=$this->__fix_collation("ALTER TABLE `".$this->table_prefix.$table."` $fields;");

                    if ($this->DB->execute($this->SQL_list[] = $SQL)) {

                        unset($this->database_fields[$table]);

                        return true;

                    } else {

                        $this->__trigger_error($this->DB->error." ($SQL)", 104);
                    }
                }
            }

        } else {

            return $this->create_database_table($table);
        }

        return false;
    }

    /**
     * Get database id's list
     *
     * @param $table string table name
     * @param $textend boolean add __conn_id__ values in output
     * @return array
     */

    public function get_database_table_ids ($table, $extend=false) {

        if (!$this->database_tables) $this->get_database_tables();

        $table    =strtolower($table);
        $ids      =array();
        $sly_table=$this->table_prefix.$table;

        if (in_array($sly_table, $this->database_tables)) {

            $SQL="select `{$table}_id` as id".($extend ? ', `__conn_id__` as cid' : '')." from `$sly_table`";

            $res=$this->DB->execute($this->SQL_list[] = $SQL);

            if ($res!==false) {

                if ($res!==true && count($res)) { foreach ($res as $v) { $ids[]=($extend ? array('conn_id'=>$v['cid'], 'id'=>$v['id']) : $v['id']); }}

                return $ids;

            } else {

                $this->__trigger_error($this->DB->error." ($SQL)", 104);
            }
        }

        return $ids;
    }

     /**
     * Get connector type
     *
     * @return string
     */

    public function get_connector_type () {

        if (!$this->database_config) $this->__get_config();

        return $this->database_config['conn_schema']['connector_type'];
    }

    /**
     * Get default language
     *
     * @return string
     */

    public function get_default_language () {

        if (!$this->database_config) $this->__get_config();

        return $this->database_config['default_language'];
    }

    /**
     * Get languages
     *
     * @return array
     */

    public function get_languages () {

        if (!$this->database_config) $this->__get_config();

        $languages=$this->database_config['languages']; $def_language=$this->get_default_language();

        if (isset($this->database_config['conn_schema']['force_output_default_language']) &&
                  $this->database_config['conn_schema']['force_output_default_language']  && !in_array($def_language, $languages)) {

              $languages[]=$def_language;
        }

        return $languages;
    }

    /**
     * Update batabase tables
     *
     * @return boolean
     */

    public function update_database_table_data ($table) {

        if (!$this->database_tables) $this->get_database_tables();

        $table=strtolower($table);

        $sly_table=$this->table_prefix.$table;

        if (in_array($sly_table, $this->database_tables)) {

            $errors    =false;
            $conn_id   =$this->database_config['conn_id'];
            $table_conn=$this->get_database_table_name($table);

            $ids=array();

            foreach ($this->get_database_table_ids($table, true) as $v) {

                $ids[$v['id']]=array($v['conn_id'], array_flip(explode(',', $v['conn_id'])));
            }

            if (count($this->get_response_table_modified_ids($table_conn))) {

                $schema=$this->get_database_table_schema       ($table);
                $data  =$this->get_response_table_modified_data($table_conn);

                if (is_array($schema) && is_array($data) && count($data)) {

                    $languages  =$this->get_languages();
                    $fields_conn=array();

                    foreach ($schema as $field=>$info) {

                        if (!empty($info['has_multilingual'])) {

                            $fields_conn[$info['name'].'_'.$info['language']]=$field;

                        } else {

                            $fields_conn[$info['name']]=$field;
                        }
                    }

                    foreach ($data as $register) {

                       $id=addslashes($register['id']);  $fields='';

                        foreach ($register as $field=>$data) {

                            if ($field=='data') {

                                foreach ($data as $field=>$value) {

                                    if (isset($fields_conn[$field])) {

                                        if (is_array($value)) {

                                            $fields.=(($fields) ? ', ' : '')."`{$fields_conn[$field]}` = '".
                                                        ($schema[$field]['type']=='list' ? addslashes(implode(',' , $value)) : @json_encode($value)).
                                                     "'";

                                        } else {

                                            $fields.=(($fields) ? ', ' : '')."`{$fields_conn[$field]}` = '".  addslashes($value)."'";
                                        }
                                    }
                                }

                            } else if (isset($fields_conn[$field])) {

                                $fields.=(($fields) ? ', ' : '')."`{$fields_conn[$field]}` = '".  addslashes($data)."'";
                            }
                        }

                        if ($fields) {

                            if (isset($ids[$id])) {

                                if (!isset($ids[$id][1][$conn_id])) { $fields.=', `__conn_id__`=\''.addslashes($ids[$id][0].','.$conn_id).'\''; }

                                $SQL="update `$sly_table` set $fields where `{$table}_id`='$id' limit 1;";

                            } else {

                                $SQL="insert into `$sly_table` set `{$table}_id`='$id', `__conn_id__`='$conn_id', $fields;";
                            }

                            if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                $this->__trigger_error($this->DB->error." ($SQL)", 104);

                                $errors=true; break;
                            }
                        }
                    }
                }
            }

            if ($dids=$this->get_response_table_deleted_ids($table)) {

                foreach ($dids as $k=>$id) {

                    if (isset($ids[$id]) && count($ids[$id][1])>1 && isset($ids[$id][1][$conn_id])) {

                        unset($ids[$id][1][$conn_id]);

                        if (count($ids[$id][1])) {

                            $SQL="update `$sly_table` set `__conn_id__`='".addslashes(implode(',', $ids[$id][1]))."' where `{$table}_id`='$id' limit 1;";

                            if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                $this->__trigger_error($this->DB->error." ($SQL)", 104);

                                $errors=true;
                            }
                        }

                        unset($dids[$k], $ids[$id]);
                    }
                }

                if (count($dids)) {

                    $SQL="delete from `$sly_table` where `{$table}_id` IN ('".implode("','", $dids)."') limit ".count($dids).';';

                    if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                        $this->__trigger_error($this->DB->error." ($SQL)", 104);

                        $errors=true;
                    }
                }
            }

            if (!$errors) return true;
        }

        return false;
    }

    /**
     * Load registers from database
     *
     * @param $table string database table
     * @param $fields array fields need
     * @param $language string language need
     * @param $conditions array for where
     * @param $force_default_language boolean include default language info
     * @param $order array list order data
     * @return array
     */

    public function extract ($table, $fields=null, $language=null, $conditions=null, $force_default_language=false, $order=null) {

        if (!$this->database_tables) $this->get_database_tables();

        $sly_table=$this->table_prefix.$table;

        if (in_array($sly_table, $this->database_tables)) {

            $language     =$this->__test_language($language);
            $base_language=$this->get_default_language();

            if ($force_default_language && $language==$base_language) { $force_default_language=false; }

            $schema=$this->get_database_table_schema($table, false);

            if ($fields!==null && !is_array($fields)) {

                $fields=null;

            } else if (count($fields)) {

                foreach ($fields as $k=>$v) { $fields[$k]=strtolower($v); }
            }

            $select=$field_title=''; $has_json_fields=0;

            if (is_array($schema) && count($schema)) {

                foreach ($schema as $field=>$info) {

                    if ($fields===null || in_array($field, $fields)) {

                        if (in_array($info['type'], array('image', 'file'))) $has_json_fields++;

                        $multi=((isset($info['has_multilingual']) && $info['has_multilingual']) ? 1 : 0);

                        if ($force_default_language && $multi && $language!=$base_language) {

                            $select .= ", IF(`{$field}_$language`!='', `{$field}_$language`, `{$field}_$base_language`) as `$field`";

                        } else {

                            $select .= ", `$field".($multi ? "_$language` as `$field" : '').'`';
                        }

                        if (preg_match('/^\w+_(title|name)(_.*)?$/', $field)) { $field_title=$field; }
                    }
                }
            }

            if ($select) {

                $where=$sql_order=''; $group_open=0;

                if (is_array($conditions)) {

                    foreach ($conditions as &$param) {

                        if (isset($param['group'])) {

                            if ($param['group']=='close') {

                                if ($group_open) { $where.=')'; } else { $group_open--; }

                            } else {

                                $where.=' '.(($where) ? ((in_array($param['group'], array('or', 'not', 'xor'))) ? $param['group'] : 'and').' ' : '').' (';

                                $group_open++;
                            }

                        } else {

                            $clause='';

                            if  (isset($param['search']) && $param['search']) {

                                $sfields=explode(',', $param['field']); $fgroup='';

                                foreach ($sfields as $field) {

                                    if (isset($schema[$field])) {

                                        if (!$db_field=$this->__get_real_field($field, $table, $language)) {

                                            $db_field =$this->__get_real_field($field, $table, $base_language);
                                        }

                                        if ($db_field) { $fgroup.=(($fgroup) ? ', ' : '')."`$db_field`"; }
                                    }
                                }

                                if ($fgroup) {

                                    $clause='lower('.((count($sfields)>1) ? "concat($fgroup)" : $fgroup).") like '%".
                                            addslashes(strtolower($param['search']))."%'";
                                }

                            } else if (isset($param['value']) && $field=$this->__get_real_field($param['field'], $table, $language)) {

                                $clause="`$field`".(($param['condition']) ? $param['condition'] : '=')."'".addslashes($param['value'])."'";

                                if ($force_default_language && isset($schema[$param['field']]['has_multilingual']) &&
                                    $schema[$param['field']]['has_multilingual']) {

                                    $clause="($clause or `".$this->__get_real_field($param['field'], $table, $base_language).'`'.
                                            (($param['condition']) ? $param['condition'] : '=')."'".addslashes($param['value'])."')";
                                }

                            }

                            if ($clause) {

                                $where.=(($where && substr($where, -1)!='(') ? ' '.(($param['logic']) ? $param['logic'] : 'and').' ' : '').$clause;
                            }
                        }
                    }

                    unset($param);
                }

                if (is_array($order)) {

                    foreach ($order as $field=>$ord) {

                        if (isset($schema[$field])) {

                               if (!$db_field=$this->__get_real_field($field, $table, $language)) {

                                $db_field =$this->__get_real_field($field, $table, $base_language);
                            }

                            if ($db_field) {

                                if (strtoupper($ord)!='ASC') { $ord='DESC'; }

                                $sql_order.=(($sql_order) ? ', ' : '')."`$db_field` $ord";
                            }
                        }
                    }
                }
                if ($field_title and !$sql_order) {

                    if (!$db_field=$this->__get_real_field($field_title, $table, $language)) {

                        $db_field =$this->__get_real_field($field_title, $table, $base_language);
                    }

                    if ($db_field) { $sql_order="`$db_field` ASC"; }
                }

                if (!$order) { $order='order by `'.$field_title.'` ASC'; }

                $SQL="select `{$table}_id` as ID, `__conn_id__` as CONN_ID$select from `$sly_table`".
                     (($where) ? ' where '.$where : '').($sql_order ? ' order by '.$sql_order : '');

                $res=$this->DB->execute($this->SQL_list[] = $SQL);

                if ($res===false) {

                    $this->__trigger_error($this->DB->error." ($SQL)", 104);

                    return false;
                }

                if (is_array($res) && count($res)) {

                    if (!isset($res[0])) $res=array($res);

                    if ($has_json_fields) {

                        foreach ($res as $k=>$data) {

                            foreach ($data as $field=>$value) {

                                if (isset($schema[$field]['type'])) {

                                    if (in_array($schema[$field]['type'], array('image', 'file'))) {

                                        $res[$k][$field]=@json_decode($value, 1);

                                    } else if ($schema[$field]['type']=='list') {

                                        $res[$k][$field]=explode(',', $value);
                                    }
                                }
                            }
                        }
                    }

                    return $res;
                }
            }
        }

        return array();
    }

    /**
     * Delete all information from database
     *
     * @param $delete_config boolean delete config table
     *
     * @return boolean
     */

    public function delete_all ($delete_config=true) {

        $this->database_tables=$this->get_database_tables();

        if (count($this->database_tables)) {

            $tables = (count($this->database_config['data_schema']) ? array_keys($this->database_config['data_schema']) : array());

            if (count($tables)) {

                if ($delete_config == true) { $tables[]=$this->table_config; }

                foreach ($tables as $table) {

                    $sly_table = $this->table_prefix.$table;

                    if (in_array($sly_table, $this->database_tables)) {

                        $SQL="DROP TABLE IF EXISTS `$sly_table`";

                        if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                            $this->__trigger_error($this->DB->error." ($SQL)", 104);
                        }

                        if (($res=array_search($sly_table, $this->database_tables)) !== false) {

                            unset($this->database_tables[$res]);
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * Delete connector info
     *
     * @param $code string connector code ID
     * @param $clean_items boolean clean connector items of the database
     *
     * @return array table => id's deleted
     */

    public function delete_connector ($code='', $clean_items=false) {

        $del_ids=array();

        if ($code && $this->database_config) {

            $SQL="delete from `".$this->table_prefix.$this->table_config."` where `conn_code`='$code' limit 1;";

            if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                $this->__trigger_error($this->DB->error." ($SQL)", 104);
            }

            if ($clean_items) {

                $tables=(count($this->database_config['data_schema']) ? array_keys($this->database_config['data_schema']) : array());

                if (count($tables)) {

                    $conn_id=$this->database_config['conn_id'];

                    foreach ($tables as $table) {

                        $sly_table=$this->table_prefix.$table;

                        $ids=array();

                        foreach ($this->get_database_table_ids($table, true) as $v) {

                            $ids[$v['id']]=array_flip(explode(',', $v['conn_id']));
                        }

                        $del_ids[$sly_table]=array_keys($ids);

                        if (count($ids)) {

                            foreach ($ids as $id=>$cons) {

                                if (count($cons)>1 && isset($cons[$conn_id])) {

                                    unset($cons[$conn_id]);

                                    if (count($cons)) {

                                        $SQL="update `$sly_table` set `__conn_id__`='".addslashes(implode(',', array_flip($cons))).
                                             "' where `{$table}_id`='$id' limit 1;";

                                        if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                            $this->__trigger_error($this->DB->error." ($SQL)", 104);

                                            $errors=true;
                                        }
                                    }

                                    unset($ids[$k]);
                                }
                            }

                            if (count($ids)) {

                                $SQL="delete from `$sly_table` where `__conn_id__`='$conn_id' limit ".count($ids).';';

                                if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                    $this->__trigger_error($this->DB->error." ($SQL)", 104);
                                }
                            }
                        }
                    }
                }
            }

            $this->database_config =
            $this->list_connectors = array();
        }

        return $del_ids;
    }

    /**
     * Get list of SQL's executed
     *
     * @return array
     */

    public function get_database_calls () {

        return $this->SQL_list;
    }

    /**
     * Set debbug
     *
     * @param $active boolean
     *
     */

    public function set_debbug ($active) {

        $this->debbug=($active ? true : false);
    }

    /**
     * Print information debbuged
     *
     */

    public function print_debbug () {

        if ($this->debbug!==false) {

            $s="\n\n[SLYR_Updater] List of SQL's:\n".print_r($this->SQL_list, 1)."\r\n";

            if ($this->debbug=='file') { file_put_contents(dirname(__FILE__).DIRECTORY_SEPARATOR.'_log_updater_'.date('Y-m-d_H-i').'.txt', $s, FILE_APPEND); }
            else                       { echo $s; }

            $this->SQL_list = array();

            return $s;
        }

        return '';
    }
}

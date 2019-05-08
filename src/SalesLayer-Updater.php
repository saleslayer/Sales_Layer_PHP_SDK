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
 * @modified 2019-05-07
 * @version 1.19
 *
 */

if                                (!class_exists('SalesLayer_Conn')) require_once dirname(__FILE__).DIRECTORY_SEPARATOR.'SalesLayer-Conn.php';
if (extension_loaded('PDO')) { if (!class_exists('slyr_SQL'))        require_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.DBPDO.php'; }
else if                           (!class_exists('slyr_SQL'))        require_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.MySQL.php';

class SalesLayer_Updater extends SalesLayer_Conn {

    public  $updater_version    = '1.19';

    public  $database           = null;
    public  $username           = null;
    public  $password           = null;
    public  $hostname           = null;
    public  $charset            = 'utf8';

    public  $table_prefix       = 'slyr_';
    public  $table_config       = '__api_config';
    public  $table_engine       = 'MyISAM';
    public  $table_row_format   = 'COMPACT';
    public  $column_key_prefix  = '';
    public  $max_column_chars   = 50;
    public  $max_table_columns  = 800;

    public  $list_connectors    = [];

    public  $DB                 = null;
    public  $SQL_list           = [];
    public  $debbug             = false; // <-- false / true / 'file'
    public  $debbug_file_path   = null;
    public  $debbug_file_prefix = '_log_updater';
    public  $test_update_stats  = null;

    private $database_tables    = null;
    private $database_fields    = [];
    private $table_columns      = [];
    private $column_tables      = [];
    private $rel_multitables    = [];
    private $database_config    = [];
    private $mysql_version      = null;

    private $database_field_types = [

        'string'    =>'text',
        'big_string'=>'mediumtext',
        'numeric'   =>'double',
        'boolean'   =>'bool',
        'image'     =>'text',
        'file'      =>'text',
        'datetime'  =>'datetime',
        'list'      =>'text',
        'key'       =>'bigint'

    ];

    private $database_field_types_charset = [

        'text'      =>'CHARACTER SET {collation}',
        'mediumtext'=>'CHARACTER SET {collation}',
        'bool'      =>'',
        'double'    =>'',
        'datetime'  =>'',
        'int'       =>'',
        'bigint'    =>'',
    ];

    /**
     * Constructor - if you're not using the class statically
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

    public function __construct ($database = null, $username = null, $password = null, $hostname = null, $codeConn = null, $secretKey = null, $SSL = false, $url = false) {

        parent::__construct();

        if ($this->__has_system_requirements() && $database != null) {

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

    public function connect ($database = null, $username = null, $password = null, $hostname = null, $codeConn = null, $secretKey = null, $SSL = false, $url = false) {

        if (!$this->response_error && $this->__has_system_requirements()) {

            $this->database_connect($database, $username, $password, $hostname);

            if (!$this->response_error) {

                parent::__construct($codeConn, $secretKey, $SSL, $url);

                return true;
            }
        }

        return false;
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

    public function database_connect ($database = null, $username = null, $password = null, $hostname = null) {

        $this->__set_database_credentials ($database, $username, $password, $hostname);

        $this->DB = new slyr_SQL($this->database, $this->username, $this->password, $this->hostname);

        if ($this->DB->error != null) {

            $this->__trigger_error ($this->DB->error, 104);

            return false;
        }

        $this->DB->execute($this->SQL_list[] = "SET NAMES '{$this->charset}';");

        $dt = new DateTime();

        $this->DB->execute($this->SQL_list[] = "SET time_zone='".$dt->format('P')."';");

        return true;
    }

    /**
     * Set the prefix for our tables if change is needed
     *
     * @param string $prefix to the tables
     * @return void
     *
     */

    public function set_table_prefix ($prefix) {

        $this->table_prefix = strtolower($prefix);
    }

    /**
     * Set the table engine if change is needed
     *
     * @param string $engine the tables should use (InnoDB or MyISAM)
     * @return void
     *
     */

    public function set_table_engine ($engine) {

        $this->table_engine = $engine;
    }

    /**
     * Set the row format for tables if change is needed
     *
     * @param string $row_format One of COMPACT, DYNAMIC or if your MySQL is set up for it, COMPRESSED
     * @return void
     *
     */

    public function set_row_format ($row_format) {

        $this->table_row_format = strtoupper($row_format);
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

    private function __set_database_credentials ($database = null, $username = null, $password = null, $hostname = null) {

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

        if (!extension_loaded('pdo') && !extension_loaded('mysql')) {

            if (!extension_loaded('pdo')) {

                self::__trigger_error ('Missing PHP PDO extension', 103);

            } else {

                self::__trigger_error ('Missing PHP MySQL extension', 103);
            }

        } else if (!extension_loaded('CURL')) {

            self::__trigger_error ('Missing CURL extension', 106);

        } else {

            return true;
        }

        return false;
    }



    /**
     * Initialize essential database tables
     *
     * @return boolean
     *
     */

    protected function __initialize_config () {

        if (!in_array($this->get_response_error(), array(103, 104))) {

            $config_table = $this->table_prefix.$this->table_config;
            
            $this->get_database_tables();

            if (!in_array($config_table, $this->database_tables)) {

                $SQL = $this->__fix_collation("CREATE TABLE IF NOT EXISTS `$config_table` (".
                    '`cnf_id` int(11) NOT NULL AUTO_INCREMENT, '.
                    '`conn_code` varchar(32) NOT NULL, '.
                    '`conn_secret` varchar(32) NOT NULL, '.
                    '`comp_id` int(11) NOT NULL, '.
                    '`last_update` int, '.
                    '`default_language` varchar(6) NOT NULL, '.
                    '`languages` varchar(512) NOT NULL, '.
                    '`conn_schema` mediumtext CHARACTER SET {collation} NOT NULL, '.
                    '`data_schema` mediumtext CHARACTER SET {collation} NOT NULL, '.
                    '`conn_extra` mediumtext CHARACTER SET {collation}, '.
                    '`updater_version` varchar(10) NOT NULL, '.
                    'PRIMARY KEY (`cnf_id`)'.
                    ') ENGINE='.$this->table_engine.' ROW_FORMAT='.$this->table_row_format.' DEFAULT CHARSET={collation} AUTO_INCREMENT=1');

                if ($this->DB->execute($this->SQL_list[] = $SQL)) {

                    $this->database_tables[] = $config_table;

                    return true;

                } else if ($this->DB->error) {

                    $this->__trigger_error($this->DB->error." ($SQL)", 104);
                }
            }
        }

        return false;
    }

    /**
     * Test if database whas initialized.
     *
     * @param string $code
     * @param boolean $refresh_info
     *
     */

    private function __test_config_initialized ($code = '', $refresh = false) {

        if (!$this->response_error) {

            $this->table_prefix = strtolower($this->table_prefix);
            $this->table_config = strtolower($this->table_config);

            $this->get_database_tables();

            if (!in_array($this->table_prefix.$this->table_config, $this->database_tables)) { $this->__initialize_config(); }

            if (!count($this->database_config)) { $this->__get_config($code, $refresh); }
        }
    }

    /**
     * Update database configurations from API response
     *
     * @return boolean
     *
     */

    private function __update_config ($update_last_upd = true) {

        if (!in_array($this->get_response_error(), array(103, 104)) && $code = addslashes($this->get_identification_code())) {

            if ($this->get_response_action() == 'refresh') {

                $table_titles     = $this->get_response_connector_schema();
                $sanitized_tables = $this->get_response_sanitized_table_names();
                $info             = $this->get_response_table_information();
                $data_schema      = [];

                if (is_array($info) and count($info)) {

                    $default_language = $this->get_response_default_language();
                    
                    foreach ($info as $table =>& $data) {

                        if (!isset($data_schema[$table]))  { 
                            
                            $data_schema[$table] = [

                                'sanitized' => $this->__clean_db_name(isset($sanitized_tables[$table]) ? $sanitized_tables[$table] : $this->__verify_table_name($table, true)),
                                'titles'    => (isset($table_titles[$table]) ? $table_titles[$table] : [ $default_language => $table ]),
                                'fields'    => []
                            ]; 
                        }

                        if (isset($data['table_joins'])) { $data_schema[$table]['table_joins'] = $data['table_joins']; }

                        foreach ($data['fields'] as $field =>& $struc) {

                            if ($field) {

                                $db_field = $this->__clean_db_name(isset($struc['sanitized']) ? $struc['sanitized'] : (isset($struc['basename']) ? $struc['basename'] : $field));

                                if (isset($struc['has_multilingual']) && $struc['has_multilingual']) {

                                    if (!isset($data_schema[$table]['fields'][$db_field])) {

                                        $data_schema[$table]['fields'][$db_field] = [

                                            'name'             => $struc['basename'],
                                            'type'             => $struc['type'],
                                            'has_multilingual' => 1,
                                            'titles'           => []
                                        ];

                                        if ($struc['type'] == 'image') {

                                            $data_schema[$table]['fields'][$db_field]['image_sizes'] = $struc['image_sizes'];
                                        }
                                    }

                                    $language = (isset($struc['language_code']) ? $struc['language_code'] : $default_language);
                                    
                                    $data_schema[$table]['fields'][$db_field]['titles'][$language] = ((isset($struc['title']) and $struc['title']) ? $struc['title'] : $db_field);

                                } else {

                                    $db_field                                         = (preg_match('/^ID_?/', $field) ? '___' : '').$db_field;
                                    $data_schema[$table]['fields'][$db_field]         = $struc;
                                    $data_schema[$table]['fields'][$db_field]['name'] = $field;
                                }
                            }
                        }
                        unset($struc);
                    }
                    unset($data);
                }
                unset($info);
            }

            $this->get_connectors_list();

            $mode = ((!isset($this->list_connectors['names']) || in_array($code, $this->list_connectors['names']))  ? 'update' : 'insert');
            $SQL  = "$mode `".                  $this->table_prefix.$this->table_config."` set ".
                    "`conn_code` = '".          $code                                                           ."', ".
                    "`conn_secret` = '".        addslashes($this->get_identification_secret())                  ."', ".
                    "`comp_id` = '".            addslashes($this->get_response_company_ID()                    )."', ".
                    ($update_last_upd ?
                       "`last_update` = '"     .addslashes($this->get_response_time(false)).                     "', " : '').
                    ($this->get_response_action() == 'refresh' ?
                       "`default_language` = '".addslashes($this->get_response_default_language()              )."', ".
                       "`languages` = '".       addslashes(implode(',', $this->get_response_languages_used())  )."', ".
                       "`conn_schema` = '".     addslashes(json_encode( $this->get_response_connector_schema()))."', ".
                       "`data_schema` = '".     addslashes(json_encode( $data_schema)                          )."', "
                       :
                       ''
                    ).
                    "`updater_version` = '".    addslashes($this->get_response_api_version()                   )."' ".
                    ($mode == 'update' ? "where `conn_code`='$code' limit 1" : '');

            if ($this->DB->execute($this->SQL_list[] = $SQL)) {

                if ($mode == 'insert') { $this->get_connectors_list($code); }

                $this->__get_config('', true);

                 return true;
            }

            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Set last updated connector
     *
     * @return bool
     */

    private function __refresh_last_update_config () {

        if ($this->get_response_time(false) && $code = addslashes($this->get_identification_code())) {

            $SQL = "update `".$this->table_prefix.$this->table_config."` set last_update='".addslashes($this->get_response_time(false))."' where conn_code='$code' limit 1";

            if ($this->DB->execute($this->SQL_list[] = $SQL)) return true;

            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Corrects name for a database table
     *
     * @return string
     */

    private function __verify_table_name ($table, $force_clean = false) {

        if ($force_clean or !isset($this->database_config['data_schema'][$table]) 
                         or !isset($this->database_config['data_schema'][$table]['sanitized'])) {

            return strtolower(preg_replace('/[^a-z0-9_\-]+/i', '_', $table));
        }

        return $this->__clean_db_name($this->database_config['data_schema'][$table]['sanitized']);
    }

    /**
     * Set connector credentials
     *
     */

    public function set_identification ($codeConn, $secretKey = null) {

        if (isset($this->database_config['conn_code']) && $codeConn != $this->database_config['conn_code']) {

            $this->database_config = [];
        }

        parent::set_identification($codeConn, $secretKey);
    }

    /**
     * Get configured connector codes
     *
     * @return array
     *
     */

    public function get_connectors_list ($code = null) {

        if (!isset($this->list_connectors['names']) || !count($this->list_connectors['names'])) {

            $this->list_connectors['names'] = [];

            $list = $this->DB->execute($this->SQL_list[] = 'select `conn_code` from `'.$this->table_prefix.$this->table_config.'`');

            if (count($list)) {

                foreach ($list as $v) { $this->list_connectors['names'][] = $v['conn_code']; }
            }
        }

        if ($code && (!count($this->list_connectors['names']) || !in_array($code, $this->list_connectors['names']))) {

            $this->list_connectors['names'][] = $code;
            $this->get_connectors_info($code);
        }

        return $this->list_connectors['names'];
    }

    /**
     * Get configured connector data
     *
     * @param string $code for get only data from specified connector
     * @param boolean $refresh_info
     * @return array
     *
     */

    public function get_connectors_info ($code = null, $refresh_info = false) {

        if ($refresh_info) { unset($this->list_connectors['data']); }

        if (!isset($this->list_connectors['data']) || !count($this->list_connectors['data']) || ($code && !isset($this->list_connectors['data'][$code]))) {

            $SQL  = 'select * from `'.$this->table_prefix.$this->table_config.'`'.
                    (isset($this->list_connectors['data'][$code]) ? ' where `conn_code`=\''.addslashes($code).'\' limit 1' : '');

            $list = $this->DB->execute($this->SQL_list[] = $SQL);

            if (count($list)) {

                if (!$code || !isset($this->list_connectors['data'])) { $this->list_connectors['data'] = array(); }

                foreach ($list as &$v) {

                    foreach ($v as &$w) { if (substr($w, 0, 1)=='{') { $w = json_decode($w, 1); }} unset($w);

                    $this->list_connectors['data'][$v['conn_code']] = $v;
                }

                unset($v, $w, $list);
            }
        }

        if (isset($this->list_connectors['data']) && !empty($this->list_connectors['data'])){

            return ($code ? (isset($this->list_connectors['data'][$code]) ? $this->list_connectors['data'][$code] : array())
                            :
                            $this->list_connectors['data']);

        } else {

            return [];
        }
    }

    /**
     * Get extra info from connector
     *
     * @param string $code connector
     * @return array
     *
     */

    public function get_connector_extra_info ($code) {

        $SQL = 'select `conn_extra` from `'.$this->table_prefix.$this->table_config.'` where `conn_code`=\''.addslashes($code).'\' limit 1';

        if ($res = $this->DB->execute($this->SQL_list[] = $SQL)) {

            return json_decode($res[0]['conn_extra'], 1);
        }

        if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

        return [];
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

    public function set_connector_extra_info ($code, $data, $refresh = false) {

        if (is_array($data)) {

            if (!$refresh) {

                $now  = $this->get_connector_extra_info($code);
                $data = array_merge((array)$now, $data);
            }

            $SQL = 'update `'.$this->table_prefix.$this->table_config.'` set `conn_extra`=\''.json_encode($data).
                   '\' where `conn_code`=\''.addslashes($code).'\' limit 1';

            if ($this->DB->execute($this->SQL_list[] = $SQL)) { return true; }

            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Set manual save last update
     *
     * @param string $code connector
     * @param integer $last_update
     *
     */

    public function set_connector_last_update ($code, $last_update = null) {

        if ($last_update == null) {

            $last_update = $this->get_response_time(false);
        }

        $SQL = 'update '.$this->table_prefix.$this->table_config.' set last_update=\''.addslashes($last_update).'\' where conn_code=\''.addslashes($code).'\' limit 1';

        if ($this->DB->execute($this->SQL_list[] = $SQL)) { return true; }

        if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

        return false;
    }

    /**
     * Get database configurations
     *
     * @return array
     *
     */

    protected function __get_config ($code = '', $refresh = false) {

        if (!in_array($this->get_response_error(), array(103, 104)) && $this->get_connectors_list()) {

            if (!$code) { $code = addslashes($this->get_identification_code()); }

            if ( $code) {

                if ($refresh || !count($this->database_config) || $this->database_config['conn_code'] != $code) {

                    $data = $this->DB->execute( $this->SQL_list[] = 'select * from `'.$this->table_prefix.$this->table_config."` where `conn_code`='$code' limit 1" );

                    if (isset($data[0])) {

                        $config = [

                            'conn_id'          =>              $data['0']['cnf_id'],
                            'conn_code'        =>              $data['0']['conn_code'],
                            'comp_id'          =>              $data['0']['comp_id'],
                            'last_update'      =>              $data['0']['last_update'],
                            'default_language' =>              $data['0']['default_language'],
                            'languages'        => explode(',', $data['0']['languages']),
                            'conn_schema'      => json_decode( $data['0']['conn_schema'], 1),
                            'data_schema'      => json_decode( $data['0']['data_schema'], 1)
                        ];

                        if (!$config['last_update']) { $config['last_update'] = null; }

                        return $this->database_config = $config;
                    }

                } else { return $this->database_config; }
            }
        }

        return null;
    }

    /**
     * Construct the real field name
     *
     * @return string
     */

    private function __get_real_field ($field, $table, $language = null) {

        $schema    = $this->get_database_table_schema($table, false);
        $db_table  = $this->__verify_table_name($table);
        $sly_table = $this->table_prefix.$db_table;
        $fields    = $this->get_database_table_fields($sly_table);

        if (is_array($schema) && (isset($schema[$field]) || isset($fields[$field]))) {

            $field = $field.((isset($schema[$field]) && isset($schema[$field]['has_multilingual']) && $schema[$field]['has_multilingual']) ? '_'.$this->__test_language($language) : '');

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

        if (is_array($language)) {

            $language = reset($language);
        }

        if (!$language or !in_array($language, $this->get_languages())) {

            $language = $this->get_default_language();
        }

        return $language;
    }

    /**
     * Get MySQL verision
     *
     * @return float number
     */

    private function __get_mysql_version () {

        if ($this->mysql_version == null) {

            $SQL = 'SHOW VARIABLES LIKE "%version%"';

            if (!($res = $this->DB->execute($this->SQL_list[] = $SQL))) {

                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

            } else {

                foreach ($res as $v) { if ($v['Variable_name'] == 'version') { $list = explode('.', $v['Value']); break; }}

                $ver = array_shift($list);

                if (count($list)) { $ver .= '.'; foreach ($list as $l) { $ver .= sprintf('%02s', $l); }}

                $this->mysql_version = floatval($ver);
            }
        }

        return $this->mysql_version;
    }

    /**
     * Define utf mode
     *
     * @return string
     */

    private function __identifies_charset_mode () {

        $ver = $this->__get_mysql_version();

        return (($ver === null || $ver < 5.0503 || $this->charset != 'utf8') ? $this->charset.' COLLATE '.$this->charset.'_general_ci'
                                                                               :
                                                                               'utf8mb4 COLLATE utf8mb4_unicode_ci');
    }

    /**
     * Fix SQL collation
     *
     * @return string
     */

    private function __fix_collation ($sql) {

        return str_replace('{collation}', $this->__identifies_charset_mode(), $sql);
    }

    /**
     * Get table internal name
     *
     * @param $table string database table
     * @return string
     *
     */

     public function get_database_table_name ($table) {

        $this->__test_config_initialized();

        return (isset($this->database_config['data_schema'][$table]['name']) ? $this->database_config['data_schema'][$table]['name'] : $table);
    }

    /**
     * Get table schema
     *
     * @param $table string database table
     * @param $extended boolean extends multilingual fields
     * @return array|boolean
     *
     */

    public function get_database_table_schema ($table, $extended = true) {

        $this->__test_config_initialized();

        $fields = [];

        if (        isset($this->database_config['data_schema'][$table]['fields'])) {

            if (    isset($this->database_config['data_schema'][$table]['table_joins'])) {

                 foreach ($this->database_config['data_schema'][$table]['table_joins'] as $field_id => $table_rel) {

                     $type     = ($this->__group_multicategory ? 'big_string' : 'key');
                     $field_db = $this->__clean_db_name($field_id);

                     $fields[$field_db] = [

                        'type'  => $type,
                        'table' => $table_rel,
                        'name'  => $field_id
                     ];
                }
            }

            if ($extended != true) {

                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {

                    if (!preg_match('/^ID_?/', $info['name'])) $fields[$field] = $info;
                }

            } else {

                $languages = $this->get_languages();

                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {

                    if (!preg_match('/^ID_?/', $info['name'])) {

                        $field_db = $this->__clean_db_name(isset($info['sanitized']) ? $info['sanitized'] : $field);
                        
                        if (isset($info['has_multilingual']) && $info['has_multilingual']) {

                            foreach ($languages as $lang) {

                                $lfield                      = $field_db.'_'.$lang;
                                $fields[$lfield]             = $info;
                                $fields[$lfield]['language'] = $lang;
                                $fields[$lfield]['basename'] = $field;
                            }

                        } else {

                            $fields[$field] = $info;
                        }
                    }
                }  
            }
        }

        return $fields;
    }

    /**
     * Get table schema database relations
     *
     * @param $table string database table
     * @return array|boolean
     *
     */

     public function get_table_fields_db_rels ($table) {

        $schema_fields = $this->get_database_table_schema($table, true);

        if (is_array($schema_fields) && count($schema_fields)) {

            foreach ($schema_fields as $db_field =>& $info) {

                $fields[$db_field] = (isset($info['basename']) ? $info['basename'] : $db_field);
            }
        }

        return $fields;
     }

    /**
     * Get table joins
     *
     * @return array
     *
     */

    public function get_database_table_joins ($table) {

        return (isset($this->database_config['data_schema'][$table]['table_joins'])) ? $this->database_config['data_schema'][$table]['table_joins'] : false;
    }

    /**
     * Test pre-update database
     *
     * @param $params array parameters
     * @param $connector_type string por special plugins
     * @param $force_refresh boolean refresh last update database
     * @return array|boolean
     *
     */

   public function test_update ($params = null, $connector_type = null, $force_refresh = false) {

        if ($code = $this->get_identification_code()) {

            $this->test_update_stats = [

                'update' => 0,
                'tables' => []
            ];

            $this->__test_config_initialized($code);

            if ($force_refresh == true || (isset($this->database_config['conn_code']) && $this->get_identification_code() != $this->database_config['conn_code'])) {

                $this->database_config['last_update'] = null;
            }

            $this->get_info($this->database_config['last_update'], $params, $connector_type);

            if (!$this->has_response_error()) {

                $this->test_update_stats['update'] = $this->get_response_time(false);

                $tables = array_keys($this->get_response_table_information());

                foreach ($tables as $table) {

                    $db_table = $this->__verify_table_name($table);

                    $this->test_update_stats['tables'][$this->table_prefix.$db_table] = [

                        'name'     =>                                        $table,
                        'modified' => $this->get_response_table_modified_ids($table),
                        'deleted'  => $this->get_response_table_deleted_ids ($table)
                    ];
                }

                return $this->test_update_stats;
            }

        } else {

            $this->__trigger_error('Invalid connector code', 2);
        }

        return false;
    }

    /**
     * Update database
     *
     * @param $params array parameters
     * @param $connector_type string for special plugins
     * @param $force_refresh boolean refresh last update database
     * @return boolean
     *
     */

    public function update ($params = null, $connector_type = null, $force_refresh = false) {

        if ($code = $this->get_identification_code()) {

            $this->__test_config_initialized($code);

            if ($force_refresh == true || (isset($this->database_config['conn_code']) && $this->get_identification_code() != $this->database_config['conn_code'])) {

                $this->database_config['last_update'] = null;
            }

            if (!isset($this->test_update_stats['update']) || !$this->test_update_stats['update']) {

                $this->get_info($this->database_config['last_update'], $params, $connector_type);

            } else {

                $this->test_update_stats = null;
            }

            if (!$this->has_response_error()) {

                $this->__update_config(false);

                if ($force_refresh == true) { $this->delete_all(false); }

                $this->get_database_tables();

                $tables = array_keys($this->get_response_table_information());
    
                foreach ($tables as $table) {

                    $db_table = $this->__verify_table_name($table);

                    if (!in_array($this->table_prefix.$db_table, $this->database_tables)) {

                        $this->create_database_table($table);

                    } else {

                        $this->update_database_table($table);
                    }
                }
            
                foreach ($tables as $table) {

                    if (count($this->get_response_table_modified_ids($table)) || count($this->get_response_table_deleted_ids($table))) {
             
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

    public function get_database_tables ($refresh = false) {

        if ($this->database_tables === null || $refresh == true) {

            $this->database_tables = [];

            $tables = $this->DB->execute($this->SQL_list[] = 'SHOW TABLES');

            if (is_array($tables) && count($tables)) {

                foreach ($tables as $v) { $this->database_tables[] = (is_array($v) ? reset($v) : $v); }
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

    public function get_database_table_fields ($db_table, $refresh = false) {

        if (!isset($this->database_fields[$db_table]) || !count($this->database_fields[$db_table]) || $refresh == true) {

            $this->database_fields[$db_table] =
            $this->rel_multitables[$db_table] =
            $this->column_tables  [$db_table] = [];

            $this->get_database_tables();

            $expr = $this->__get_table_match($db_table);

            foreach ($this->database_tables as $test_db_table) {

                if (preg_match($expr, $test_db_table)) {

                    $this->rel_multitables   [$db_table][] = $test_db_table;    
                    $this->table_columns[$test_db_table]   = [];
                    
                    $data = $this->DB->execute($this->SQL_list[] = 'SHOW COLUMNS FROM `'.$test_db_table.'`');

                    if (is_array($data) && count($data)) {

                        foreach ($data as $v) { 

                            $type = preg_replace('/^([^\s\(]+).*$/', '\\1', $v['Type']);
                        
                            $this->database_fields[$db_table][$v['Field']] = ($type == 'tinyint' ? 'bool' : $type);
                            $this->table_columns  [$test_db_table][]       = $v['Field'];
                            
                            if (!isset($this->column_tables[$db_table][$v['Field']])) {
                                
                                       $this->column_tables[$db_table][$v['Field']] = $test_db_table;
                            }
                        }
                    }
                }
            }
        }

        return $this->database_fields[$db_table];
    }

    /**
     * Clean cache for updates in the database structure
     *
     */

    public function clean_table_cache ($table = '', $add_prefix = true) {

        if (!$table) {

            $this->table_columns   =
            $this->database_fields =
            $this->rel_multitables = [];    

        } else {

            $db_table  = $this->__verify_table_name($table);
            $sly_table = ($add_prefix ? $this->table_prefix : '').$db_table;

            $this->get_database_table_fields($sly_table);

            if (isset($this->database_fields[$sly_table])) {

                unset($this->database_fields[$sly_table], $this->rel_multitables[$sly_table]);

                if (is_array($this->table_columns) and count($this->table_columns)) {
                
                    $expr = $this->__get_table_match($sly_table);

                    foreach (array_keys($this->table_columns) as $test_db_table) {

                        if (preg_match($expr, $test_db_table)) unset($this->table_columns[$test_db_table]);    
                    }
                }
            }
        }
    }

    /**
     * For multi-table
     *
     * @return string
     */

    private function __get_table_match ($db_table) {

        return '/^'.preg_quote($db_table, '/').'(___[0-9]+)?$/';
    }

    /**
     * Get database last update
     *
     * @return string
     */

    public function get_database_last_update ($mode = 'datetime') {

        $this->__test_config_initialized();

        $time = (isset($this->database_config['last_update']) ? $this->database_config['last_update'] : null);

        return  ((isset($time) and $mode == 'datetime') ? date('Y-m-d H:i:s', $time) : $time);
    }

    /**
     * Get database connector codename
     *
     * @return string
     */

    public function get_database_connector_code () {

        $this->__test_config_initialized();

        return (isset($this->database_config['conn_code']) ? $this->database_config['conn_code'] : null);
    }

    /**
     * Test if table exist in the database
     *
     * @param string $table table name
     * @return boolean
     */

    public function has_database_table ($table) {

        $db_table = $this->__verify_table_name($table);

        return (isset($this->database_tables[$this->table_prefix.$db_table]) ? true : false);
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
     
     * @param $table_name string table name
     * @return boolean
     */

    public function create_database_table ($table) {

        $db_table = $this->__verify_table_name($table);

        $this->get_database_tables();

        if (!in_array($this->table_prefix.$db_table, $this->database_tables)) {

            return $this->__alter_table($table, true);

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

    public function update_database_table ($table) {

        $db_table  = $this->__verify_table_name($table);
        $sly_table = $this->table_prefix.$db_table;

        $this->get_database_tables();

        if (in_array($sly_table, $this->database_tables)) {

            if ($this->get_response_action() == 'refresh') {

                $SQL = (count($this->get_connectors_list()) == 1 ?

                            "TRUNCATE `".   $sly_table."`;"
                            :
                            "DELETE FROM `".$sly_table."` WHERE fin_in_set('".$this->database_config['conn_id']."', `__conn_id__`);");

                $this->DB->execute($this->SQL_list[] = $SQL);
            }

            $schema_db_fields = $this->get_table_fields_db_rels($table);

            if (is_array($schema_db_fields) && count($schema_db_fields)) {

                $this->get_database_table_fields($sly_table);

                if (count($this->get_connectors_list()) == 1) {

                    $field_id = $this->__get_field_key($db_table);
                    $fields   = [];

                    foreach (array_keys($this->database_fields[$sly_table]) as $db_field) {

                        if (!in_array($db_field, [$field_id, '__conn_id__']) && !array_key_exists($db_field, $schema_db_fields)) {

                            foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                                if (in_array($db_field, $this->table_columns[$multi_db_table])) {

                                    $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."DROP `$db_field`";

                                    break;
                                }
                            }
                        }
                    }

                    if (count($fields)) {

                        foreach ($fields as $multi_db_table => $string_fields) {

                            $SQL = $this->__fix_collation("ALTER TABLE `".$multi_db_table."` $string_fields;");

                            $this->DB->execute($this->SQL_list[] = $SQL);
                        }

                        $this->clean_table_cache($sly_table);
                    }
                }

                return $this->__alter_table($table);
            }

        } else {

            return $this->create_database_table($table);
        }

        return false;
    }

    /**
     * Get field key
     *
     */

     private function __get_field_key ($db_table) {

        return $this->column_key_prefix.$db_table.'_id';
    }

    /**
     * Create insert string for key field 
     */

     private function __get_field_key_for_insert ($table, $primary = true) {

        $db_table = $this->__verify_table_name($table);
        $field_id = $this->__get_field_key ($db_table);

        return '`'.$field_id.'` int unsigned not null'.($primary ? ' auto_increment primary key, `__conn_id__` varchar(512) NOT NULL' 
                                                                   : 
                                                                   ', UNIQUE KEY `'.$field_id.'` (`'.$field_id.'`)');
    }

    /**
     * Create table
     *
     * @param $table_name string table name
     * @return boolean
     */

    private function __create_table ($db_table, $string_fields) {

        if ($db_table) {

            $this->DB->execute($this->SQL_list[] = "DROP TABLE IF EXISTS `$db_table`");

            $SQL = $this->__fix_collation("CREATE TABLE `$db_table` ($string_fields) ENGINE=".$this->table_engine.' ROW_FORMAT='.$this->table_row_format.
                                          ' DEFAULT CHARSET={collation} AUTO_INCREMENT=1');   

            if ($this->DB->execute($this->SQL_list[] = $SQL)) {
                
                $this->database_tables[]            = $db_table;
                $this->rel_multitables[$db_table][] = $db_table; 

                return true;
            }

            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Create string for database alter tables
     *
     * @return boolean
     */

    private function __alter_table ($table, $mode_insert = false) {

        $schema = $this->get_database_table_schema($table);
        $fields = [];

        if (is_array($schema) && count($schema)) {

            $ok        = true;
            $db_table  = $this->__verify_table_name($table);
            $sly_table = $this->table_prefix.$db_table;

            $this->get_database_table_fields($sly_table);

            if ($mode_insert) {
      
                if (!isset($this->rel_multitables[$sly_table]) || !in_array($sly_table, $this->rel_multitables[$sly_table])) {

                    $key_field = $this->__get_field_key_for_insert($table);
                    $ok        = $this->__create_table($sly_table, $key_field);

                    $this->table_columns  [$sly_table]   = [];
                    $this->rel_multitables[$sly_table][] = $sly_table;
                }
            }

            if ($ok) {

                foreach ($schema as $field =>& $info) {

                    if ($info && !$info['key']) {

                        $db_field = $this->__clean_db_name(isset($info['sanitized']) ? $info['sanitized'] : $field);
                        $type     = $this->__get_database_type_schema($info['type']);
                        $mode     = (($mode_insert or !isset($this->database_fields[$sly_table][$db_field])) ?
                                                    'ADD' : ($this->database_fields[$sly_table][$db_field] != $type ? "CHANGE `$db_field` " : ''));

                        if ($mode) {

                            $this_db_table = '';

                            foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                                if (count($this->table_columns[$multi_db_table]) < $this->max_table_columns) {

                                    $this_db_table                         = $multi_db_table; 
                                    $this->table_columns[$this_db_table][] = $db_field;

                                    break;
                                }
                            }

                            if (!$this_db_table) {

                                $count_multi_tables                      = count($this->rel_multitables[$sly_table]);
                                $this_db_table                           = $sly_table.($count_multi_tables ? '___'.$count_multi_tables : '');
                                $this->table_columns  [$this_db_table][] = $db_field;
                                $this->rel_multitables[$sly_table]    [] = $this_db_table;

                                $key_field = $this->__get_field_key_for_insert($table, false);

                                $this->__create_table($this_db_table, $key_field);
                            }

                            $fields[$this_db_table] .= ($fields[$this_db_table] ? ', ' : '')."$mode `$db_field` $type ".($type == 'bigint' ? ' UNSIGNED' : '').
                                                       $this->database_field_types_charset[$type];
                        }
                    }
                }

                if (count($fields)) {

                    foreach ($fields as $this_db_table => $string_fields) {

                        $SQL = $this->__fix_collation("ALTER TABLE `".$this_db_table."` $string_fields;");   
                        
                        if (!$this->DB->execute($this->SQL_list[] = $SQL)) { $ok = false; break; }  
                    }

                    $this->clean_table_cache($sly_table);

                    if (!$ok && $this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
                }
            }

            if ($ok) {

                return true;
            }
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

    public function get_database_table_ids ($table, $extend = false) {

        $this->get_database_tables();

        $db_table  = $this->__verify_table_name($table);
        $ids       = [];
        $sly_table = $this->table_prefix.$db_table;

        if (in_array($sly_table, $this->database_tables)) {

            $SQL = 'select `'.$this->__get_field_key($db_table).'` as id'.($extend ? ', `__conn_id__` as cid' : '')." from `$sly_table`";
            $res = $this->DB->execute($this->SQL_list[] = $SQL);

            if ($res !== false) {

                if ($res !== true && count($res)) { foreach ($res as $v) { $ids[] = ($extend ? [ 'conn_id' => $v['cid'], 'id'=> $v['id'] ] : $v['id']); }}

                return $ids;
            }

            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return $ids;
    }

     /**
     * Get connector type
     *
     * @return string
     */

    public function get_connector_type () {

        $this->__test_config_initialized();

        return $this->database_config['conn_schema']['connector_type'];
    }

    /**
     * Get default language
     *
     * @return string
     */

    public function get_default_language () {

        $this->__test_config_initialized();

        return $this->database_config['default_language'];
    }

    /**
     * Get languages
     *
     * @return array
     */

    public function get_languages () {

        $this->__test_config_initialized();

        $languages    = $this->database_config['languages'];
        $def_language = $this->get_default_language();

        if (isset($this->database_config['conn_schema']['force_output_default_language']) &&
                  $this->database_config['conn_schema']['force_output_default_language']  && !in_array($def_language, $languages)) {

              $languages[] = $def_language;
        }

        return $languages;
    }

    /**
     * Get language titles of the table
     *
     * @param string $table
     * @return array
     *
     */

     function get_table_title ($language, $table) {

        if ($table) {

            if (isset($this->database_config['data_schema'][$table])) {
            
                $table_info =& $this->database_config['data_schema'][$table];

                if (isset($table_info['titles'])) {

                    $default_language = $this->get_default_language();

                    if ( isset($table_info['titles'][$language])) {

                        return $table_info['titles'][$language];

                    } else if ( isset($table_info['titles'][$default_language])) {

                        return        $table_info['titles'][$default_language]; 
                    }
                }

                return (isset($table_info['name']) ? $table_info['name'] : $table);
            }
        }

        return '';
    }

    /**
     * Get language titles of the table
     *
     * @param string $table
     * @return array
     *
     */

     function get_language_titles_of_table ($table) {

        $languages = $this->database_config['languages'];

        if ($table) {

            if (isset($this->database_config['data_schema'][$table])) {

                $table_info =& $this->database_config['data_schema'][$table];
                $table_name =  (isset($table_info['name']) ? $table_info['name'] : $table);

                if (isset($table_info['titles']) and isset($table_info['titles'])) {

                    $table_titles     =& $this->database_config['data_schema'][$table]['titles'];
                    $default_language =  $this->get_default_language();
                    $default_title    =  (isset($table_info['titles'][$default_language]) ? $table_info['titles'][$default_language] : $table_name);
                    $titles           =  []; 

                    foreach ($languages as $lang) { $titles[$lang] = (isset($table_titles[$lang]) ? $table_titles[$lang] : $default_title); }

                    return $titles;
                }

                $titles = []; foreach ($languages as $lang) { $titles[$lang] = $table_name; }

                return $titles;
            }      
        }

        return [];
    }

    /**
     * Get field titles of table
     *
     * @param string $table
     * @return array
     *
     */

    public function get_language_titles_of_fields ($table = null) {

        $titles = [];
        
        $this->__test_config_initialized();
    
        if (!$table) {
        
            $tables = array_keys($this->database_config['data_schema']);
        
        } else {
        
            $tables = [ $table ];
        }

        $languages = $this->get_languages();
        
        foreach ($tables as $table) {
        
            $titles[$table] = [];
            
            if (       isset($this->database_config['data_schema'][$table]) 
                and is_array($this->database_config['data_schema'][$table]['fields']) 
                and    count($this->database_config['data_schema'][$table]['fields'])) {
            
                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {
                
                    if (!preg_match('/^___id/', $field)) {
          
                        if (isset($info['titles']) and count($info['titles'])) {
                        
                            $titles[$table][$field] = $info['titles'];
                            
                        } else {
                        
                            if (!isset($titles[$table][$field])) $titles[$table][$field] = [];
                         
                            foreach ($languages as $lang) { 
                            
                                $titles[$table][$field][$lang] = $field;
                            }
                        }
                    }
                }
                
                unset($info);
            }
        }
   
        return $titles;
    }

    /**
     * Get field titles of table
     *
     * @param string $language (ISO 639-1)
     * @return array
     *
     */

     public function get_titles_of_fields ($language, $table = null) {

        $titles = [];
        
        $this->__test_config_initialized();
    
        if (!$table) {
        
            $tables = array_keys($this->database_config['data_schema']);
        
        } else {
        
            $tables = [ $table ];
        }

        $default_language = $this->get_default_language();
        
        foreach ($tables as $table) {
        
            $titles[$table] = [];
            
            if (       isset($this->database_config['data_schema'][$table]) 
                and is_array($this->database_config['data_schema'][$table]['fields']) 
                and    count($this->database_config['data_schema'][$table]['fields'])) {
            
                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {
                
                    if (!preg_match('/^___id/', $field)) {
          
                        if (isset($info['titles']) and count($info['titles'])) {
                        
                            if (isset($info['titles'][$language])) {
                            
                                $titles[$table][$field] = $info['titles'][$language];
                            
                            } else if (isset($info['titles'][$default_language])) {
                            
                                $titles[$table][$field] = $info['titles'][$default_language];
                                
                            } else {
                            
                                $titles[$table][$field] = reset($info['titles']);
                            }    
                        
                        } else {
                   
                            $titles[$table][$field] = $field;
                        }
                    }
                }
                
                unset($info);
            }
        }
   
        return $titles;
    }

    /**
     * Get field titles in certain language
     *
     * @param string $language (ISO 639-1)
     * @return array
     *
     */

    public function get_field_title ($language, $field, $table) {

        if ($field and $table) {

            if (isset($this->database_config['data_schema'][$table]) and isset($this->database_config['data_schema'][$table])) {

                if (isset($this->database_config['data_schema'][$table]['fields'][$field])) {

                    $field_info =& $this->database_config['data_schema'][$table]['fields'][$field];

                    if (isset($field_info['titles'])) {

                        $default_language = $this->get_default_language();

                        if ( isset($field_info['titles'][$language])) {    

                            return $field_info['titles'][$language];

                        } else if (isset($field_info['titles'][$default_language])) {    

                            return       $field_info['titles'][$default_language];
                        }
                    }

                    return $field_info['name'];
                }
            }
        }
        
        return $field;
    }
        
    /**
     * Update batabase tables
     *
     * @return boolean
     */

    public function update_database_table_data ($table) {

        $this->get_database_tables();

        $db_table  = $this->__verify_table_name($table);
        $sly_table = $this->table_prefix.$db_table;

        if (in_array($sly_table, $this->database_tables)) {

            $errors     = false;
            $conn_id    = $this->database_config['conn_id'];
            $ids        = [];

            foreach ($this->get_database_table_ids($table, true) as $v) {

                $ids[$v['id']] = [$v['conn_id'], array_flip(explode(',', $v['conn_id']))];
            }
            
            $ok_modifications = count($this->get_response_table_modified_ids($table));
            $ids_deleted      =       $this->get_response_table_deleted_ids ($table);
            $ok_deletes       = count($ids_deleted);

            if ($ok_modifications || $ok_deletes) {

                $this->get_database_table_fields($sly_table);
            }

            if ($ok_modifications) {

                $schema = $this->get_database_table_schema       ($table);
                $data   = $this->get_response_table_modified_data($table);

                if (is_array($schema) && count($schema) && is_array($data) && count($data)) {

                    $fields_conn = [];

                    foreach ($schema as $field => $info) {

                        $db_field = $this->__clean_db_name(isset($info['sanitized']) ? $info['sanitized'] : $field);

                        if (!empty($info['has_multilingual'])) {

                            $fields_conn[$info['name'].'_'.$info['language']] = $db_field;

                        } else {

                            $fields_conn[$info['name']] = $db_field;
                        }
                    }

                    foreach ($data as $k =>& $register) {

                       $id     = addslashes($register['id']);
                       $fields = [];

                       foreach ($register as $field =>& $f_data) {

                            if ($field == 'data') {

                                foreach ($f_data as $d_field =>& $value) {

                                    if (isset($fields_conn[$d_field]) && isset($schema[$fields_conn[$d_field]])) {

                                        $db_field       = $fields_conn[$d_field];
                                        $multi_db_table = $this->column_tables[$sly_table][$db_field];

                                        if (!$multi_db_table) $multi_db_table = $sly_table;

                                        if (is_array($value)) {

                                            $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."`{$fields_conn[$d_field]}` = '".
                                                                        ($schema[$d_field]['type'] == 'list' ? addslashes(implode(',' , $value)) : json_encode($value))."'";

                                        } else {

                                            $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."`{$fields_conn[$d_field]}` = ".
                                                                        (empty($value) ? 'null' : "'".addslashes($value)."'");
                                        }
                                    }
                                }

                                unset($value);

                            } else if (isset($fields_conn[$field]) && $field != 'id') {

                                if (is_array($f_data)) { $f_data = implode(',', $f_data); }

                                $db_field       = $this->__clean_db_name(isset($schema[$field]['sanitized']) ? $schema[$field]['sanitized'] : $field);
                                $multi_db_table = $this->column_tables[$sly_table][$db_field]; 
                                
                                if (!$multi_db_table) $multi_db_table = $sly_table;

                                $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."`{$fields_conn[$field]}` = '".addslashes($f_data)."'";
                            }

                            unset($register[$field]);
                        }

                        unset($register, $data[$k], $f_data);

                        if (count($fields)) {

                            $ok       = true;
                            $field_id = $this->__get_field_key($db_table);

                            if (isset($ids[$id])) {

                                $limit     = (count($fields) > 1 ? '' : ' limit 1');
                                $tables    =
                                $db_fields = '';

                                foreach ($fields as $multi_db_table => $string_fields) {

                                    $db_fields .= ($db_fields ? ', ' : '').$string_fields;
                             
                                    if (!isset($ids[$id][1][$conn_id]) &&  $multi_db_table == $sly_table) { 
                                        
                                        $db_fields .= ', `__conn_id__`=\''.addslashes($ids[$id][0].','.$conn_id).'\'';
                                    }

                                    $tables .= ($tables ? ' left join `'.$multi_db_table.'` using(`'.$field_id.'`)' : '`'.$multi_db_table.'`');
                                }

                                $SQL = "update $tables set $db_fields where `$field_id`='$id'$limit;";

                                if (!$this->DB->execute($this->SQL_list[] = $SQL)) $ok = false;

                            } else {

                                foreach ($fields as $multi_db_table => $string_fields) {

                                    if ($multi_db_table == $sly_table) $string_fields .= ", `__conn_id__`='$conn_id'";

                                    $SQL = "insert into `$multi_db_table` set `$field_id`='$id', $string_fields;";

                                    if (!$this->DB->execute($this->SQL_list[] = $SQL) && $this->DB->error) $ok = false;
                                }
                            }

                            unset($fields);

                            if (!$ok) {

                                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                                $errors = true;
                            }
                        }
                    }
                }
            }

            if ($ok_deletes) {

                $field_id = $this->__get_field_key($db_table);

                foreach ($ids_deleted as $k => $id) {

                    if (isset($ids[$id]) && count($ids[$id][1]) > 1 && isset($ids[$id][1][$conn_id])) {

                        unset($ids[$id][1][$conn_id]);

                        if (count($ids[$id][1])) {

                            $SQL = "update `$sly_table` set `__conn_id__`='".addslashes(implode(',', $ids[$id][1]))."' where `$field_id`='$id' limit 1;";

                            if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                                $errors = true;
                            }
                        }

                        unset($ids_deleted[$k], $ids[$id]);
                    }
                }

                if ($num_deletes = count($ids_deleted)) {

                    foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                        $SQL = "delete from `$multi_db_table` where `$field_id` IN ('".implode("','", $ids_deleted)."') limit ".count($num_deletes).';';

                        if (!$this->DB->execute($this->SQL_list[] = $SQL) && $multi_db_table == $sly_table) {

                            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                            $errors = true;
                        }
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

    public function extract ($table, $fields = null, $language = null, $conditions = null, $force_default_language = false, $order = null, $get_internal_ids = false) {

        $this->get_database_tables();

        $sly_table = $this->table_prefix.$table;

        if (in_array($sly_table, $this->database_tables)) {

            $language      = $this->__test_language($language);
            $base_language = $this->get_default_language();

            if ($force_default_language && $language == $base_language) { $force_default_language = false; }

            $schema = $this->get_database_table_schema($table, false);

            if ($fields !== null && !is_array($fields)) {

                $fields = null;

            } else if (count($fields)) {

                foreach ($fields as $k => $v) { $fields[$k] = strtolower($v); }
            }

            $select          =
            $field_title     = '';
            $has_json_fields = 0;
            $tables_db       = [];

            if (is_array($schema) && count($schema)) {

                $db_table  = $this->__verify_table_name($table);
                $sly_table = $this->table_prefix.$db_table;

                $this->get_database_table_fields($sly_table);

                if ($fields === null) $fields = array_keys($schema);

                foreach ($fields as $field) {

                    if (isset($schema[$field])) {

                        $info =& $schema[$field];
        
                        if (in_array($info['type'], [ 'image', 'file' ])) ++ $has_json_fields;

                        $multi = ((isset($info['has_multilingual']) && $info['has_multilingual']));
      
                        if ($force_default_language && $multi && $language != $base_language) {

                            $field_db      = $field.'_'.$language;
                            $field_db_base = $field.'_'.$base_language;
                            $table_db      = $this->__get_table_for_field($field_db,      $table);
                            $table_db_base = $this->__get_table_for_field($field_db_base, $table);

                            $select .= ($select ? ', ' : '')."IF(`$table_db`.`$field_db`!='', `$table_db`.`$field_db`, `$table_db_base`.`$field_db_base`) as `$field`";

                            if (!isset($tables_db[$table_db]))                                     $tables_db[$table_db]      = 1;
                            if ($table_db != $table_db_base && !isset($tables_db[$table_db_base])) $tables_db[$table_db_base] = 1;

                        } else {

                            $field_db = $field.($multi ? '_'.$language : '');
                            $table_db = $this->__get_table_for_field($field_db, $table);

                            $select .= ($select ? ', ' : '')." `$table_db`.`$field_db`".($multi ? " as `$field`" : '');

                            if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;
                        }

                        unset($info);

                        if (preg_match('/^\w+_(title|name)(_.*)?$/', $field)) { $field_title = $field; }
                    }
                }
            }

            if ($select) {

                $where      =
                $sql_order  = '';
                $group_open = 0;

                if (is_array($conditions)) {

                    foreach ($conditions as &$param) {

                        if (isset($param['group'])) {

                            if ($param['group'] == 'close') {

                                if ($group_open) $where .= ')'; else -- $group_open;

                            } else {

                                $where .= ' '.($where ? (in_array($param['group'], array('or', 'not', 'xor')) ? $param['group'] : 'and').' ' : '').' (';

                                ++ $group_open;
                            }

                        } else {

                            $clause = '';

                            if  (isset($param['search']) && $param['search']) {

                                $sfields = explode(',', $param['field']);
                                $fgroup  = '';

                                foreach ($sfields as $field) {

                                    if (isset($schema[$field])) {

                                        if (!$field_db = $this->__get_real_field($field, $table, $language)) {

                                            $field_db  = $this->__get_real_field($field, $table, $base_language);
                                        }

                                        if ($field_db) { 
                                            
                                            $table_db = $this->__get_table_for_field($field_db, $table);
                                            $fgroup  .= ($fgroup ? ', ' : '')."`$table_db`.`$field_db`";

                                            if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;
                                        }
                                    }
                                }

                                if ($fgroup) {

                                    $clause = 'lower('.((count($sfields) > 1) ? "concat($fgroup)" : $fgroup).") like '%".addslashes(strtolower($param['search']))."%'";
                                }

                            } else if (isset($param['value']) && $field_db = $this->__get_real_field($param['field'], $table, $language)) {

                                $table_db = $this->__get_table_for_field($field_db, $table);
                                $clause   = "`$table_db`.`$field_db`".(($param['condition']) ? $param['condition'] : '=')."'".addslashes($param['value'])."'";

                                if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;

                                if (   $force_default_language
                                    && isset($schema[$param['field']]['has_multilingual'])
                                    &&       $schema[$param['field']]['has_multilingual']
                                    && $field_db = $this->__get_real_field($param['field'], $table, $base_language)) {

                                    $table_db = $this->__get_table_for_field($field_db, $table);
                                    $clause   = "($clause or `$table_db`.`$field_db`".($param['condition'] ? $param['condition'] : '=')."'".addslashes($param['value'])."')";

                                    if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;
                                }
                            }

                            if ($clause) {

                                $where .= (($where && substr($where, -1)!='(') ? ' '.(($param['logic']) ? $param['logic'] : 'and').' ' : '').$clause;
                            }
                        }
                    }

                    unset($param);
                }

                if (is_array($order)) {

                    foreach ($order as $field => $ord) {

                        if (isset($schema[$field])) {

                               if (!$field_db = $this->__get_real_field($field, $table, $language)) {

                                    $field_db = $this->__get_real_field($field, $table, $base_language);
                            }

                            if ($field_db) {

                                if (strtoupper($ord) != 'ASC') { $ord = 'DESC'; }

                                $table_db   = $this->__get_table_for_field($field_db, $table);
                                $sql_order .= ($sql_order ? ', ' : '')."`$table_db`.`$db_field` $ord";

                                if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;
                            }
                        }
                    }
                }
                if ($field_title and !$sql_order) {

                    if (!$db_field = $this->__get_real_field($field_title, $table, $language)) {

                        $db_field  = $this->__get_real_field($field_title, $table, $base_language);
                    }

                    if ($db_field) { 
                        
                        $table_db  = $this->__get_table_for_field($field_db, $table);
                        $sql_order = "`$table_db`.`$db_field` ASC";
                    
                        if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;
                    }
                }

                if ($field_title and $sql_order == '') {
                    
                    $table_db  = $this->__get_table_for_field($field_title, $table);
                    $sql_order = "`$table_db`.`$field_title` ASC";

                    if (!isset($tables_db[$table_db])) $tables_db[$table_db] = 1;
                }

                $field_id      = $this->__get_field_key($table_db);
                $string_tables = '';

                foreach (array_keys($tables_db) as $table) {

                    $string_tables .= ($string_tables ? " left join `$table` using(`$field_id`)" : "`$table`");
                }

                $SQL = 'select '.($get_internal_ids ? "`$field_id` as ID, `__conn_id__` as CONN_ID, " : '')."$select from $string_tables".
                       ($where ? ' where '.$where : '').($sql_order ? ' order by '.$sql_order : '');

                $res = $this->DB->execute($this->SQL_list[] = $SQL);

                if ($res === false) {

                    if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                    return false;
                }
    
                if (is_array($res) && count($res)) {

                    if (!isset($res[0])) { $res = array($res); }

                    if ($has_json_fields) {

                        foreach ($res as $k =>& $data) {

                            foreach ($data as $field => $value) {

                                if (isset($schema[$field]['type'])) {

                                    if (in_array($schema[$field]['type'], [ 'image', 'file' ])) {

                                        $res[$k][$field] = json_decode($value, 1);

                                    } else if ($schema[$field]['type'] == 'list') {

                                        $res[$k][$field] = explode(',', $value);
                                    }
                                }
                            }
                        }
                    }

                    return $res;
                }
            }
        }

        return [];
    }

    /** 
     *  Gets a multi-table name from a field
     */

    private function __get_table_for_field ($db_field, $table) {

        if ($table) {

            $db_table  = $this->__verify_table_name($table);
            $sly_table = $this->table_prefix.$db_table;

            if (!isset($this->column_tables[$sly_table])) $this->get_database_table_fields($sly_table);

            if ( isset($this->column_tables[$sly_table][$db_field])) {

                return $this->column_tables[$sly_table][$db_field];
            }

            return $table;
        }
        return '';

    }

    /**
     * Delete all information from database
     *
     * @param $delete_config boolean delete config table
     *
     * @return boolean
     */

    public function delete_all ($delete_config = true) {

        $this->get_database_tables();

        if (count($this->database_tables)) {

            $tables = (count($this->database_config['data_schema']) ? array_keys($this->database_config['data_schema']) : []);

            if (count($tables)) {

                if ($delete_config == true) { $tables[] = $this->table_config; }

                foreach ($tables as $table) {

                    $sly_table = $this->table_prefix.$table;

                    if (in_array($sly_table, $this->database_tables)) {

                        $SQL = "DROP TABLE IF EXISTS `$sly_table`";

                        if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
                        }

                        if (($res = array_search($sly_table, $this->database_tables)) !== false) {

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

    public function delete_connector ($code = '', $clean_items = false) {

        $del_ids = [];

        $this->__test_config_initialized();

        if ($this->__get_config($code) !== null) {

            $SQL = "delete from `".$this->table_prefix.$this->table_config."` where `conn_code`='$code' limit 1;";

            if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
            }

            if ($clean_items && $this->response_error!=104) {

                $tables = (count($this->database_config['data_schema']) ? array_keys($this->database_config['data_schema']) : []);

                if (count($tables)) {

                    $conn_id = $this->database_config['conn_id'];

                    foreach ($tables as $table) {

                        $db_table  = $this->__verify_table_name($table);
                        $sly_table = $this->table_prefix.$db_table;
                        $ids       = [];

                        foreach ($this->get_database_table_ids($table, true) as $v) {

                            $ids[$v['id']] = array_flip(explode(',', $v['conn_id']));
                        }

                        $del_ids[$sly_table] = array_keys($ids);

                        if (count($ids)) {

                            $field_id = $this->__get_field_key($db_table);

                            foreach ($ids as $id => $cons) {

                                if (count($cons) > 1 && isset($cons[$conn_id])) {

                                    unset($cons[$conn_id]);

                                    if (count($cons)) {

                                        $SQL = "update `$sly_table` set `__conn_id__`='".addslashes(implode(',', array_flip($cons)))."' where `$field_id`='$id' limit 1;";

                                        if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
                                        }
                                    }

                                    unset($ids[$id]);
                                }
                            }

                            if (count($ids)) {

                                $this->get_database_table_fields($sly_table);

                                $where = '`'.$field_id.'` IN ('.implode(', ', $ids).')';

                                foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                                    $SQL = "delete from `$multi_db_table` where $where;";

                                    if (!$this->DB->execute($this->SQL_list[] = $SQL)) {

                                        if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            $this->database_config =
            $this->list_connectors = [];
        }

        return $del_ids;
    }
    
    /**
     * function array_join 
     * merges 2 arrays preserving the keys,
     */

    function array_join ($a1, $a2) { 

        foreach ($a2 as $key => $value) $a1[$key] = $value; 

        return $a1; 
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

        $this->debbug = ($active !== false ? ($active == 'file' ? 'file' : true) : false);
    }

    /**
     * Print information debbuged
     *
     */

    public function print_debbug () {

        if ($this->debbug !== false) {

            $s = "\n\n[SLYR_Updater] List of SQL's:\n".print_r($this->SQL_list, 1)."\r\n";

            if ($this->debbug == 'file') { file_put_contents($this->get_path_log_debbug().$this->debbug_file_prefix.date('_Y-m-d_H-i').'.txt', $s, FILE_APPEND); }
            else                         { echo $s; }

            $this->SQL_list = [];

            return $s;
        }

        return '';
    }

    /**
     * Get path to save the logs
     *
     */

    public function get_path_log_debbug () {

        if (!isset($this->debbug_file_path)) {

            $this->debbug_file_path = dirname(__FILE__).DIRECTORY_SEPARATOR;
        }

        return $this->debbug_file_path;
    }

    /**
     * Set path to save the logs
     *
     */

    public function set_path_log_debbug ($path) {

        $this->debbug_file_path = $path;
    }

    /**
     * Convert string to octal
     *
     */

     private function __to_hex ($string) {

        $sum = 0;
        $len = strlen($string);

        for ($i = 0; $i < $len; $i ++) {

            $sum += ord($string[$i]);
        }

        return strtolower(dechex($sum));
    }

    /**
     * Clean field name for database
     *
     */

    private function __clean_db_name ($field) {

        $field = strtolower(preg_replace(['/[^a-z0-9_\-]+/', '/_{2,}/'], '_', $field));

        if (($max = strlen($field)) > ($db_max = ($this->max_column_chars - 5)) && ($max - $db_max) > 5) {
                        
            $field = substr($field, 0, $this->max_column_chars).'_'.$this->__to_hex(substr($field, $this->max_column_chars));
        } 
        
        return $field;
    }

    /**
     * Set the error code and message.
     *
     * @param string $message error text
     * @param int    $errnum  error identificator
     */
     public function __trigger_error ($message, $errnum) {

         if ($errnum == 104) $this->SQL_list[] = "ERROR $errnum: $message";
          
         parent::__trigger_error($message, $errnum);
     }

     /**
      * Clean cache
      *
      */

    public function clean_cache () {

        $this->response_error    = false;
        $this->database_config   = 
        $this->list_connectors   = [];
        $this->database_tables   =
        $this->test_update_stats = null;

        $this->clean_table_cache();
    }
}

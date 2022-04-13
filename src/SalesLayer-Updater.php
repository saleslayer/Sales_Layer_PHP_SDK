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
 * @modified 2022-04-13
 * @version 1.27
 *
 */

if                                (!class_exists('SalesLayer_Conn')) require_once dirname(__FILE__).DIRECTORY_SEPARATOR.'SalesLayer-Conn.php';
if (extension_loaded('PDO')) { if (!class_exists('slyr_SQL'))        require_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.DBPDO.php'; }
else if                           (!class_exists('slyr_SQL'))        require_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.MySQL.php';

class SalesLayer_Updater extends SalesLayer_Conn {

    public  $updater_version    = '1.27';

    public  $database           = null;
    public  $username           = null;
    public  $password           = null;
    public  $hostname           = null;
    public  $charset            = 'utf8';

    public  $table_prefix       = 'slyr_';
    public  $table_config       = '__api_config';
    public  $table_engine       = 'MyISAM';
    public  $table_row_format   = 'COMPACT';
    public  $max_column_chars   = 50;
    public  $max_table_columns  = 800;
    public  $max_size_multikey  = 512;

    public  $list_connectors    = [];

    public  $DB                 = null;
    public  $SQL_list           = [];
    public  $debug              = false; // <-- false / true / 'file'
    public  $debug_file_path    = null;
    public  $debug_file_prefix  = '_log_updater';
    public  $debug_max_list     = 500;
    public  $test_update_stats  = null;

    private $database_tables    = null;
    private $database_fields    = [];
    private $table_columns      = [];
    private $column_tables      = [];
    private $rel_multitables    = [];
    private $database_config    = [];
    private $mysql_version      = null;
    private $update_pagination  = null;
    private $database_init_date = null;
    private $SQL_errors         = [];

    private $database_field_types = [

        'string'    =>'text',
        'big_string'=>'mediumtext',
        'numeric'   =>'double',
        'boolean'   =>'bool',
        'image'     =>'text',
        'file'      =>'text',
        'datetime'  =>'datetime',
        'list'      =>'text',
        'key'       =>'bigint',
        'multi-key' =>'bigint'

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

    private $debbug_file_name;

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

    public function __construct ($database = null, $username = null, $password = null, $hostname = null, $codeConn = null, $secretKey = null, $SSL = null, $url = null) {

        parent::__construct();

        $this->set_path_log_debug();
        
        if ($database != null) {

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

    public function connect ($database = null, $username = null, $password = null, $hostname = null, $codeConn = null, $secretKey = null, $SSL = null, $url = null) {

        if (!$this->response_error && $this->__has_system_requirements()) {

            $this->database_connect($database, $username, $password, $hostname);
            
            if (!$this->response_error) {

                if ($codeConn) {
                    $this->set_identification($codeConn, $secretKey);
                }
    
                if ($SSL !== null) $this->set_SSL_connection($SSL);
                if ($url !== null) $this->set_URL_connection($url);

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

        $this->__set_database_credentials($database, $username, $password, $hostname);

        $this->DB = new slyr_SQL($this->database, $this->username, $this->password, $this->hostname);

        if ($this->DB->error != null) {

            $this->__trigger_error($this->DB->error, 104);

            return false;
        }

        $this->DB->execute($this->__add_to_debug("SET NAMES '{$this->charset}';"));

        $dt = new DateTime();

        $this->DB->execute($this->__add_to_debug("SET time_zone='".$dt->format('P')."';"));

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

        if ($database != null) { $this->database = $database; }
        if ($database != null) { $this->username = $username; }
        if ($database != null) { $this->password = $password; }
        if ($database != null) { $this->hostname = $hostname; }
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

                self::__trigger_error('Missing PHP PDO extension', 103);

            } else {

                self::__trigger_error('Missing PHP MySQL extension', 103);
            }

        } else if (!extension_loaded('CURL')) {

            self::__trigger_error('Missing CURL extension', 106);

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
                        '`languages` varchar('.$this->max_size_multikey.') NOT NULL, '.
                        '`conn_schema` mediumtext CHARACTER SET {collation} NOT NULL, '.
                        '`data_schema` mediumtext CHARACTER SET {collation} NOT NULL, '.
                        '`conn_extra` mediumtext CHARACTER SET {collation}, '.
                        '`updater_version` varchar(10) NOT NULL, '.
                        'PRIMARY KEY (`cnf_id`)'.
                        ') ENGINE='.$this->table_engine.' ROW_FORMAT='.$this->table_row_format.' DEFAULT CHARSET={collation} AUTO_INCREMENT=1');

                if ($this->DB->execute($this->__add_to_debug($SQL))) {

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

            if (empty($this->database_config)) { $this->__get_config($code, $refresh); }
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

            $this->get_connectors_list();

            $mode    = ((!isset($this->list_connectors['names']) || in_array($code, $this->list_connectors['names']))  ? 'update' : 'insert');
            $refresh = ($mode == 'insert' or $this->get_response_action() == 'refresh');
            $hash    = '';

            if (!$refresh && $mode == 'update' && $this->__get_config($code) !== null) {

                if (isset($this->database_config['conn_schema']['hash'])) {

                    $hash    = md5(json_encode($this->get_schema_information()));
                    $refresh = ($hash != $this->database_config['conn_schema']['hash']);

                } else {

                    $refresh = true;
                }
            }

            if ($refresh) {

                $table_titles        = $this->get_response_table_titles();
                $sanitized_tables    = $this->get_response_sanitized_table_names();
                $info                = $this->get_response_table_information();
                $conn_schema         = $this->get_response_connector_schema();
                $conn_schema['hash'] = ($hash ? $hash : md5(json_encode($info)));
                $data_schema         = [];

                if (is_array($info) and !empty($info)) {

                    if (!$hash) $hash = md5(json_encode($info));

                    $default_language = $this->get_response_default_language();
            
                    foreach ($info as $table =>& $data) {

                        if (!isset($data_schema[$table])) { 
                            
                            $data_schema[$table] = [

                                'sanitized' => $this->__clean_db_name(isset($sanitized_tables[$table]) ? $sanitized_tables[$table] : $this->__verify_table_name($table, true)),
                                'titles'    => (isset($table_titles[$table]) ? $table_titles[$table] : [ $default_language => $table ]),
                                'fields'    => []
                            ]; 
                        }

                        if (isset($data['table_joins'])) { $data_schema[$table]['table_joins'] = $data['table_joins']; }
                    }
                    unset($data);

                    foreach ($info as $table =>& $data) {

                        foreach ($data['fields'] as $field =>& $struc) {

                            if ($field) {
                  
                                $is_key = (in_array($struc['type'], ['key', 'multi-key']) or substr($field, 0, 3) == 'ID_');

                                if (!$is_key) {
                                    
                                    if ($field == 'REF') {

                                        $db_field = '___'.$data_schema[$table]['sanitized'].'_ref';
    
                                    } else {

                                        $db_field = $this->__clean_db_name(isset($struc['sanitized']) ? $struc['sanitized'] : (isset($struc['basename']) ? $struc['basename'] : $field));
                                    }
                                }

                                if (!$is_key && isset($struc['has_multilingual']) && $struc['has_multilingual']) {

                                    if (!isset($data_schema[$table]['fields'][$db_field])) {

                                        $data_schema[$table]['fields'][$db_field] = [

                                            'name'             => $struc['basename'],
                                            'type'             => $struc['type'],
                                            'has_multilingual' => 1,
                                            'titles'           => []
                                        ];
                                    }

                                    $language = (isset($struc['language_code']) ? $struc['language_code'] : $default_language);
                                    
                                    $data_schema[$table]['fields'][$db_field]['titles'][$language] = ((isset($struc['title']) and $struc['title']) ? $struc['title'] : $db_field);

                                } else {

                                    if ($is_key) $db_field = $this->__get_db_key_from_field($field, $table, $data_schema);

                                    $data_schema[$table]['fields'][$db_field] = [

                                        'name'   => $field,
                                        'type'   => $struc['type'],
                                        'titles' => (isset($struc['titles']) ? $struc['titles'] : [ $default_language => $struc['title'] ]),
                                    ];
                                    
                                    if(isset($struc['tag_translations'])){
                                        
                                        $data_schema[$table]['fields'][$db_field]['tag_translations'] = $struc['tag_translations'];
                                    }
                                }

                                if ($struc['type'] == 'image') {

                                    $data_schema[$table]['fields'][$db_field]['image_sizes'] = $struc['image_sizes'];
                                }
                            }
                        } 
                        unset($struc);
                    }
                    unset($data);
                }
                unset($info);
            }

            $SQL  = "$mode `".                  $this->table_prefix.$this->table_config."` set ".
                    "`conn_code` = '".          $code."', ".
                    "`conn_secret` = '".        addslashes($this->get_identification_secret())."', ".
                    "`comp_id` = '".            addslashes($this->get_response_company_ID())."', ".
                    ($update_last_upd ?
                       "`last_update` = '"     .addslashes($this->get_response_time(false))."', " : '').
                    ($refresh ?
                       "`default_language` = '".addslashes($this->get_response_default_language())."', ".
                       "`languages` = '".       addslashes(implode(',', (array)$this->get_response_languages_used()))."', ".
                       "`conn_schema` = '".     addslashes(json_encode($conn_schema))."', ".
                       "`data_schema` = '".     addslashes(json_encode($data_schema))."', "
                       :
                       ''
                    ).
                    "`updater_version` = '".    addslashes($this->get_response_api_version() )."' ".
                    ($mode == 'update' ? "where `conn_code`='$code' limit 1" : '');

            if ($this->DB->execute($this->__add_to_debug($SQL))) {

                if ($mode == 'insert') { $this->get_connectors_list($code); }

                if ($refresh or !isset($this->database_config['conn_code'])) { 
                    
                    $this->__get_config('', true); 
                
                } else {

                    $this->database_config['conn_code']       = $code;
                    $this->database_config['conn_secret']     = $this->get_identification_secret();
                    $this->database_config['comp_id']         = $this->get_response_company_ID();
                    $this->database_config['updater_version'] = $this->get_response_api_version();

                    if ($update_last_upd) $this->database_config['last_update'] = $this->get_response_time(false);
                }

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

            if ($this->DB->execute($this->__add_to_debug($SQL))) return true;

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

        if (!is_array($this->list_connectors) || !isset($this->list_connectors['names'])) {

            $this->list_connectors['names'] = [];

            $list = $this->DB->execute($this->__add_to_debug('select `conn_code` from `'.$this->table_prefix.$this->table_config.'`'));

            if (!empty($list)) {

                foreach ($list as $v) { $this->list_connectors['names'][] = $v['conn_code']; }
            }
        }

        if ($code && (empty($this->list_connectors['names']) || !in_array($code, $this->list_connectors['names']))) {

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

        if (!isset($this->list_connectors['data']) || empty($this->list_connectors['data']) || ($code && !isset($this->list_connectors['data'][$code]))) {

            $SQL  = 'select * from `'.$this->table_prefix.$this->table_config.'`'.
                    (isset($this->list_connectors['data'][$code]) ? ' where `conn_code`=\''.addslashes($code).'\' limit 1' : '');

            $list = $this->DB->execute($this->__add_to_debug($SQL));

            if (!empty($list)) {

                if (!$code || !isset($this->list_connectors['data'])) { $this->list_connectors['data'] = []; }

                foreach ($list as &$v) {

                    foreach ($v as &$w) { if (substr($w, 0, 1) == '{') { $w = json_decode($w, 1); }} unset($w);

                    $this->list_connectors['data'][$v['conn_code']] = $v;
                }

                unset($v, $w, $list);
            }
        }

        if (isset($this->list_connectors['data']) && !empty($this->list_connectors['data'])) {

            return ($code ? (isset($this->list_connectors['data'][$code]) ? $this->list_connectors['data'][$code] : [])
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

        if ($res = $this->DB->execute($this->__add_to_debug($SQL))) {

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

            if ($this->DB->execute($this->__add_to_debug($SQL))) { return true; }

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

        if ($this->DB->execute($this->__add_to_debug($SQL))) { return true; }

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

                if ($refresh || empty($this->database_config) || $this->database_config['conn_code'] != $code) {

                    if (!$refresh and isset($this->list_connectors['data'][$code])) {

                        $data = $this->list_connectors['data'][$code];

                    } else {

                        $data = $this->DB->execute($this->__add_to_debug('select * from `'.$this->table_prefix.$this->table_config."` where `conn_code`='$code' limit 1"));

                        if (isset($data[0])) {

                            $data                = $data[0];
                            $data['conn_schema'] = json_decode($data['conn_schema'], 1);
                            $data['data_schema'] = json_decode($data['data_schema'], 1);

                        } else {

                            $data = null;
                        }
                    }

                    if (is_array($data)) {

                        $this->database_config = [

                            'conn_id'          =>              $data['cnf_id'],
                            'conn_code'        =>              $data['conn_code'],
                            'comp_id'          =>              $data['comp_id'],
                            'last_update'      =>              $data['last_update'],
                            'default_language' =>              $data['default_language'],
                            'languages'        => explode(',', $data['languages']),
                            'conn_schema'      =>              $data['conn_schema'],
                            'data_schema'      =>              $data['data_schema']
                        ];

                        if (!$this->database_config['last_update']) { $this->database_config['last_update'] = null; }

                        return $this->database_config;
                    }
                
                } else { 
                    
                    return $this->database_config; 
                }
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

            $db_field = $field.((isset($schema[$field]) && isset($schema[$field]['has_multilingual']) && $schema[$field]['has_multilingual']) ? '_'.$this->__test_language($language) : '');

            if (isset($fields[$db_field])) { return $db_field; }
        }

        return (isset($fields[$field]) ? $field : '');
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

            if (!($res = $this->DB->execute($this->__add_to_debug($SQL)))) {

                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

            } else {

                foreach ($res as $v) { if ($v['Variable_name'] == 'version') { $list = explode('.', $v['Value']); break; }}

                $ver = array_shift($list);

                if (!empty($list)) { $ver .= '.'; foreach ($list as $l) { $ver .= sprintf('%02s', $l); }}

                $this->mysql_version = floatval($ver);
            }
        }

        return $this->mysql_version;
    }

    /**
     * Get charset
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
     * Get collate
     *
     * @return string
     */

    private function __get_collate () {

        $ver = $this->__get_mysql_version();

        return (($ver === null || $ver < 5.0503 || $this->charset != 'utf8') ? $this->charset.'_general_ci' : 'utf8mb4_unicode_ci');
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
     * Get table list
     *
     * @param $lang string language
     * @return array
     *
     */

    public function get_tables ($lang = '') {

        $this->__test_config_initialized();

        $def_lang = $this->get_default_language();

        if (   isset($this->database_config['conn_schema']['language_table_names'])) {

            $tables = [];

            foreach ($this->database_config['conn_schema']['language_table_names'] as $table => $titles) {

                $tables[$table] = (($lang and isset($titles[$lang]) and $titles[$lang]) ? $titles[$lang] : $titles[$def_lang]);
            }

            return $tables;
        }

        return $this->get_tables_sanitized();
    }

    /**
     * Get table list with sanitized names
     *
     * @return array
     *
     */

     public function get_tables_sanitized () {

        $this->__test_config_initialized();

        if ( isset($this->database_config['conn_schema']['sanitized_table_names'])) {

            return $this->database_config['conn_schema']['sanitized_table_names'];
        }

        return [];
    }

    /**
     * Get internal table name
     *
     * @param $table table alias or internal
     * @return array
     *
     */

     public function get_internal_table_name ($table, $lang = '') {

        $tables = $this->get_tables($lang);

        if (!empty($tables)) {

            if (isset($tables[$table])) return $table;

            if (($key_table = array_search($table, $tables)) !== false) return $key_table;
        }

        return $table;
    }

    /**
     * Get table internal name
     *
     * @param $table string database table
     * @return string
     *
     */

     public function get_database_table_name ($table) {

        $table = $this->get_internal_table_name($table);

        return (isset($this->database_config['data_schema'][$table]['name']) ? $this->database_config['data_schema'][$table]['name'] : $table);
    }

    /**
     * Get the real name of the table in the database
     *
     * @param $table string database table
     * @return string
     *
     */

     public function get_database_table_db_name ($table, $add_prefix = true) {

        $table = $this->get_internal_table_name($table);

        if (isset($this->database_config['data_schema'][$table])) {

            return ($add_prefix ? $this->table_prefix : '').$this->__clean_db_name(isset($this->database_config['data_schema'][$table]['sanitized'] ) ? 
                                                                                         $this->database_config['data_schema'][$table]['sanitized'] : $table);
        }
        
        return false;
    }

    /**
     * Test if table exists
     *
     * @param $table string database table
     * @return boolean
     *
     */

     public function table_schema_exists ($table) {

        $this->__test_config_initialized();

        if ($table and isset($this->database_config['data_schema'][$table]) 
                   and isset($this->database_config['data_schema'][$table]['fields'])) {

            return true;
        }

        return false;
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

        if ($table and isset($this->database_config['data_schema'][$table]) 
                   and isset($this->database_config['data_schema'][$table]['fields'])) {

            if ($extended != true) {

                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {

                    $fields[$field] = $info;
                }

            } else {

                $languages = $this->get_languages();

                foreach ($this->database_config['data_schema'][$table]['fields'] as $field_db =>& $info) {

                    if (isset($info['has_multilingual']) && $info['has_multilingual']) {

                        foreach ($languages as $lang) {

                            $lfield                      = $field_db.'_'.$lang;
                            $fields[$lfield]             = $info;
                            $fields[$lfield]['language'] = $lang;
                            $fields[$lfield]['basename'] = $field_db;
                        }

                    } else {

                        $fields[$field_db] = $info;
                    }
                }  
            }
        }

        return $fields;
    }

    /**
     * Get database fields of table
     *
     * @param $table string database table
     * @return array|boolean
     *
     */

     public function get_table_fields_db ($table) {

        $data_schema = $this->get_database_table_schema($table);

        return ((is_array($data_schema) && !empty($data_schema)) ? array_keys($data_schema) : []);

    }

    /**
     * Get table schema database relations
     *
     * @param $table string database table
     * @return array|boolean
     *
     */

     public function get_table_fields_db_rels ($table) {

        $fields      = [];
        $data_schema = $this->get_database_table_schema($table);

        if (is_array($data_schema) && !empty($data_schema)) {

            foreach ($data_schema as $field =>& $info) {

                if ($field !== 'ID_PARENT' and (in_array($info['type'], ['key', 'multi-key']) or substr($field, 0, 3) == 'ID_')) {

                    $fields[$field] = $this->__get_db_table_from_key($field);
                }
            }
        }

        return $fields;
    }

    /** 
     * Get the field ID of table
     *  
     * @param $table string database table
     * @return string
     */

     public function get_db_field_ID ($table) {

        return $this->get_db_field_by_name('ID', $table);
    } 

    /** 
     * Get the field parent of categorization table
     *  
     * @param $table string database table
     * @return string
     */

     public function get_db_field_parent_ID ($table) {

        return $this->get_db_field_by_name('ID_PARENT', $table);
    }

    /** 
     * Get the database field from name
     *  
     * @param $table string database table
     * @return string
     */

    public function get_db_field_by_name ($name, $table) {

        if ($name && $table) {

            $data_schema = $this->get_database_table_schema($table);

            if (is_array($data_schema) && !empty($data_schema)) {    

                foreach ($data_schema as $field => $info) {
                    
                    if ($info['name'] == $name) {

                        return $field;
                    }    
                }
            }
        }

        return '';
    }

    /**
     * Get database table joins from all tables
     *
     * @return array
     *
     */

     public function get_all_table_joins () {

        $this->__test_config_initialized();

        $list = [];

        foreach ($this->database_config['data_schema'] as $table =>& $table_info) {

            if (   isset($table_info['table_joins']) && !empty($table_info['table_joins'])) {

                $list[$table] = [];

                foreach ($table_info['table_joins'] as $field => $table_join) {
    
                    $list[$table][$table_join] = $field;
                }
            }
        }

        return $list;
    }

    /**
     * Get database table joins from table name
     *
     * @return array
     *
     */

    public function get_database_table_joins ($table) {

        $this->__test_config_initialized();

        $list = [];

        if (   isset($this->database_config['data_schema'][$table]['table_joins'])) {

            foreach ($this->database_config['data_schema'][$table]['table_joins'] as $field => $table) {

                      $sly_table  = $this->table_prefix.$this->__verify_table_name($table);
                $list[$sly_table] = $this->__get_db_key_from_field($field, $table);
            }
        }

        return $list;
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
     * Set pagination for updates
     *
     * @param $length integer pagination length
     *
     */

    public function set_update_pagination ($length) {

        $this->update_pagination = $length;
    }

    /**
     * Get pagination for updates
     *
     * @return integer pagination length
     *
     */

    public function get_update_pagination () {

        return ($this->update_pagination ? $this->update_pagination : 0);
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

    public function update ($params = null, $connector_type = null, $force_refresh = false, $status_function = null) {

        if ($code = $this->get_identification_code()) {

            $this->__test_config_initialized($code);

            if (    $force_refresh == true 
                || !isset($this->database_config['last_update']) 
                || (isset($this->database_config['conn_code']) && $this->get_identification_code() != $this->database_config['conn_code'])) {

                $this->database_config['last_update'] = null;
            }
   
            if (!isset($this->test_update_stats['update']) || !$this->test_update_stats['update']) {

                if ($this->update_pagination && !isset($params['pagination'])) {

                    if (is_null($params)) $params = [];

                    $params['pagination'] = $this->update_pagination;
                }

                $this->get_info($this->database_config['last_update'], $params, $connector_type);

            } else {

                $this->test_update_stats = null;
            }

            if (!$this->response_error) {

                $this->__update_config(false);

                if (!$this->response_error) {

                    if ($force_refresh == true) { $this->delete_all(false); }

                    $this->get_database_tables();

                    $tables     = array_keys($this->get_response_table_information());
                    $new_tables = [];

                    foreach ($tables as $table) {

                        $db_table = $this->__verify_table_name($table);

                        if (!in_array($this->table_prefix.$db_table, $this->database_tables)) {

                            $this->create_database_table($table);

                            $new_tables[] = $table;

                        } else {

                            $this->update_database_table($table);
                        }
                    }

                    $this->database_init_date = '';
            
                    do {

                        if (is_array($this->response_tables_data)) {

                            $page_tables = array_keys($this->response_tables_data);

                            foreach ($page_tables as $table) {

                                if (!empty($this->get_response_table_modified_ids($table)) || !empty($this->get_response_table_deleted_ids($table))) {
                        
                                    $this->update_database_table_data($table);
                                }
                            }
                        }
                        
                        if ($status_function) call_user_func_array($status_function, [ $this ]);

                    } while ($this->get_next_page_info());

                    if ($this->get_response_action() == 'refresh') {

                        foreach ($tables as $table) {

                            if (!in_array($table, $new_tables)) {

                                $this->clean_database_table_updated($table);
                            }
                        }
                    }

                    $this->__refresh_last_update_config();

                    return true;
                }
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

            if (!$this->response_error) {

                $tables = $this->DB->execute($this->__add_to_debug('SHOW TABLES'));

                if (is_array($tables) && !empty($tables)) {

                    foreach ($tables as $v) { $this->database_tables[] = (is_array($v) ? reset($v) : $v); }
                }
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

        if (!isset($this->database_fields[$db_table]) || empty($this->database_fields[$db_table]) || $refresh == true) {

            $this->database_fields[$db_table] =
            $this->rel_multitables[$db_table] =
            $this->column_tables  [$db_table] = [];

            $this->get_database_tables();

            $expr = $this->__get_table_match($db_table);

            foreach ($this->database_tables as $test_db_table) {

                if (preg_match($expr, $test_db_table)) {

                    $this->rel_multitables   [$db_table][] = $test_db_table;    
                    $this->table_columns[$test_db_table]   = [];
                    
                    $data = $this->DB->execute($this->__add_to_debug('SHOW COLUMNS FROM `'.$test_db_table.'`'));

                    if (is_array($data) && !empty($data)) {

                        foreach ($data as $v) { 

                            $this->database_fields[$db_table][$v['Field']] = $this->__get_field_type_for_verify($v['Type']); 
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
     * Clean database field type
     *
     */

    private function __get_field_type_for_verify ($type) {

        $type = preg_replace('/^([^\s\(]+).*$/', '\\1', $type);
                        
        return ($type == 'tinyint' ? 'bool' : $type);
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

            $db_table = $this->__verify_table_name($table);

            if ($add_prefix and !isset($this->database_fields[$db_table])) {

                $db_table = $add_prefix.$db_table;
            }
            if (isset($this->database_fields[$db_table])) {

                unset($this->database_fields[$db_table], $this->rel_multitables[$db_table]);

                if (is_array($this->table_columns) and !empty($this->table_columns)) {
                
                    $expr = $this->__get_table_match($db_table);

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

        if ($this->get_response_action()) {

            $db_table  = $this->__verify_table_name($table);
            $sly_table = $this->table_prefix.$db_table;

            $this->get_database_tables();

            if (in_array($sly_table, $this->database_tables)) {

                $schema_db_fields = $this->get_table_fields_db($table);

                if (is_array($schema_db_fields) && !empty($schema_db_fields)) {

                    $fields = [];

                    $this->get_database_table_fields($sly_table);

                    if (!in_array('___modified', $this->table_columns[$sly_table])) {

                        if (!isset($fields[$sly_table])) $fields[$sly_table] = '';

                        $fields[$sly_table]                .= "ADD `___modified` DATETIME NOT NULL";
                        $this->table_columns[$sly_table][]  = '___modified';
                    }

                    if (count($this->get_connectors_list()) == 1) {

                        $field_id = $this->__get_field_key($db_table);
                        
                        foreach (array_keys($this->database_fields[$sly_table]) as $db_field) {

                            if (!in_array($db_field, [$field_id, '__conn_id__', '___modified']) && !in_array($db_field, $schema_db_fields)) {

                                foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                                    if (isset($this->table_columns[$multi_db_table]) && in_array($db_field, $this->table_columns[$multi_db_table])) {

                                        if (!isset($fields[$multi_db_table])) $fields[$multi_db_table] = '';

                                        $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."DROP `$db_field`";

                                        break;
                                    }
                                }
                            }
                        }
                    }

                    if (!empty($fields)) {

                        foreach ($fields as $multi_db_table => $string_fields) {

                            $SQL = $this->__fix_collation("ALTER TABLE `".$multi_db_table."` $string_fields;");

                            $this->DB->execute($this->__add_to_debug($SQL));
                        }

                        $this->clean_table_cache($sly_table);
                    }

                    return $this->__alter_table($table);
                }

            } else {

                return $this->create_database_table($table);
            }
        }

        return false;
    }

    /**
     * Get database field key from field name
     * 
     * @param $field string field name
     * @param $table string table name
     * @return string
     */

    private function __get_db_key_from_field ($field, $table, $data_schema = null) {

        if ($data_schema === null) $data_schema =& $this->database_config['data_schema'];

        if (substr($field, 0, 3) == 'ID_' and $field != 'ID_PARENT') {

            $table_rel = preg_replace('/^ID_/u', '', $field);
            $table_rel = ((    isset($data_schema[$table_rel]) 
                           and isset($data_schema[$table_rel]['sanitized'])) ? $data_schema[$table_rel]['sanitized'] : preg_replace('/^ID_(.+)$/u', '\\1', $field));

        } else {

            $table_rel = $data_schema[$table]['sanitized'].($field != 'ID' ? '_parent' : ''); 
        }

        return '___'.$table_rel.'_id';
    }

    /**
     *  Get table name from field key
     *
     * @param $field string field name
     * @param $table string table name
     * @return string
     */

    private function __get_db_table_from_key ($field) {

        if (substr($field, 0, 3) == '___') {

            return preg_replace('/^___(.+)?(_parent)?_id$/u', '\\1', $field);
        }

        return preg_replace('/^ID_/u', '', $field);
    }

    /**
     * Get database field key
     *
     */

     private function __get_field_key ($db_table) {

        return '___'.$db_table.'_id';
    }

    /**
     * Create insert string for key field 
     */

     private function __get_field_key_for_insert ($table, $primary = true) {

        $db_table = $this->__verify_table_name($table);
        $field_id = $this->__get_field_key ($db_table);

        return '`__conn_id__` varchar('.$this->max_size_multikey.') NOT NULL, `'.$field_id.'` bigint unsigned not null'.
               ($primary ? ' auto_increment primary key' : ', UNIQUE KEY `'.$field_id.'` (`'.$field_id.'`)');
    }

    /**
     * Create table
     *
     * @param $table_name string table name
     * @return boolean
     */

    private function __create_table ($db_table, $string_fields, $auto_increment = true) {

        if ($db_table) {

            $this->DB->execute($this->__add_to_debug("DROP TABLE IF EXISTS `$db_table`"));

            $SQL = $this->__fix_collation("CREATE TABLE `$db_table` ($string_fields) ENGINE=".$this->table_engine.' ROW_FORMAT='.$this->table_row_format.
                                          ' DEFAULT CHARSET={collation}'.($auto_increment ? ' AUTO_INCREMENT=1' : ''));

            if ($this->DB->execute($this->__add_to_debug($SQL))) {
                
                $db_table_base                           = preg_replace('/___[0-9]+$/', '', $db_table);
                $this->database_tables[]                 = $db_table;
                $this->table_columns  [$db_table_base]   = [];
                $this->rel_multitables[$db_table_base][] = $db_table; 

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

        if (is_array($schema) && !empty($schema)) {

            $ok        = true;
            $db_table  = $this->__verify_table_name($table);
            $sly_table = $this->table_prefix.$db_table;

            $this->get_database_table_fields($sly_table);

            if ($mode_insert) {
      
                if (!isset($this->rel_multitables[$sly_table]) || !in_array($sly_table, $this->rel_multitables[$sly_table])) {

                    $string_fields = $this->__get_field_key_for_insert($table);
                    $ok            = $this->__create_table($sly_table, $string_fields);
                }
            }

            if ($ok) {
                
                $fields    = [];
                $key_field = $this->__get_field_key($db_table);

                if (!in_array('___modified', $this->table_columns[$sly_table])) {

                    if (!isset($fields[$sly_table])) $fields[$sly_table] = '';

                    $fields[$sly_table]                .= "ADD `___modified` DATETIME NOT NULL";
                    $this->table_columns[$sly_table][]  = '___modified';
                }

                foreach ($schema as $db_field =>& $info) {

                    if ($db_field != $key_field && is_array($info) && !empty($info)) {

                        $type = $this->__get_database_type_schema($info['type']);
           
                        if ($this->__group_multicategory == true && $info['type'] == 'multi-key') {

                            $type = 'varchar('.$this->max_size_multikey.')';
                        }

                        $mode = (($mode_insert or !isset($this->database_fields[$sly_table][$db_field])) ?
                                                'ADD' : ($this->database_fields[$sly_table][$db_field] != $this->__get_field_type_for_verify($type) ? "CHANGE `$db_field` " : ''));

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

                                $this->__create_table($this_db_table, '`'.$key_field.'` bigint unsigned not null', false);
                            }

                            if (!isset($this->database_fields[$sly_table][$db_field]) || preg_match('/^CHANGE\s+/i', $mode)) {

                                if (!isset($fields[$this_db_table])) $fields[$this_db_table] = '';

                                $fields[$this_db_table] .= ($fields[$this_db_table] ? ', ' : '')."$mode `$db_field` $type ".($type == 'bigint' ? ' UNSIGNED' : '').
                                                           (isset($this->database_field_types_charset[$type]) ? $this->database_field_types_charset[$type] : '');
                            }
                        }
                    }
                }
                unset($info);

                if (!empty($fields)) {

                    foreach ($fields as $this_db_table => $string_fields) {

                        $SQL = $this->__fix_collation("ALTER TABLE `".$this_db_table."` $string_fields;");   
                        
                        if (!$this->DB->execute($this->__add_to_debug($SQL))) { $ok = false; break; }  
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
            $res = $this->DB->execute($this->__add_to_debug($SQL));

            if ($res !== false) {

                if ($res !== true && !empty($res)) { foreach ($res as $v) { $ids[] = ($extend ? [ 'conn_id' => $v['cid'], 'id'=> $v['id'] ] : $v['id']); }}

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
     * Get if we need autocomplete multi-lingual data
     *
     * @return string
     */

     public function need_autocomplete_language () {
 
        $this->__test_config_initialized();

        return ($this->database_config['conn_schema']['correct_language'] ? true : false);
    }

    /**
     * Get default language
     *
     * @return string
     */

    public function get_default_language () {

        $this->__test_config_initialized();

        if ((   !isset($this->database_config['conn_schema']['force_output_default_language']) ||
                      !$this->database_config['conn_schema']['force_output_default_language']) && 
             !in_array($this->database_config['default_language'], $this->database_config['languages'])) {

            return reset($this->database_config['languages']);
        }

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
            
            if (      isset($this->database_config['data_schema'][$table]) 
                && is_array($this->database_config['data_schema'][$table]['fields']) 
                &&   !empty($this->database_config['data_schema'][$table]['fields'])) {
            
                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {
                
                    if (!preg_match('/^___id/', $field)) {
          
                        if (isset($info['titles']) and !empty($info['titles'])) {
                        
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
            
            if (      isset($this->database_config['data_schema'][$table]) 
                && is_array($this->database_config['data_schema'][$table]['fields']) 
                &&   !empty($this->database_config['data_schema'][$table]['fields'])) {
            
                foreach ($this->database_config['data_schema'][$table]['fields'] as $field =>& $info) {
                
                    if (!preg_match('/^___id/', $field)) {
          
                        if (isset($info['titles']) and !empty($info['titles'])) {
                        
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
     * Get list field values translated
     *
     * @param string $language (ISO 639-1)
     * @return array
     *
     */

    public function get_list_field_values_translations ($language, $field, $table, $values) {
        
        $this->__test_config_initialized();
        
        if (!is_array($values)) {
            
            $values           = preg_split('/\s*,\s*/', $values, -1, PREG_SPLIT_NO_EMPTY);
            $return_as_string = true;

        } else {
            
            $return_as_string = false;
        }

        if ($field and $table) {

            if (isset($this->database_config['data_schema'][$table]) and isset($this->database_config['data_schema'][$table])) {

                if (isset($this->database_config['data_schema'][$table]['fields'][$field])) {

                    $field_info =& $this->database_config['data_schema'][$table]['fields'][$field];

                    if (isset($field_info['tag_translations'])) {

                        $default_language = $this->get_default_language();
                        $result           = [];

                        foreach($values as $k => $v) {
                         
                            if ( isset($field_info['tag_translations'][$language][$v])) {    

                                $result[$k] = $field_info['tag_translations'][$language][$v];

                            } else if ( isset($field_info['tag_translations'][$default_language][$v])) {    

                                $result[$k] = $field_info['tag_translations'][$default_language][$v];
                                
                            } else {
                                
                                $result[$k] = $v;  
                            }   
                        }
                        
                        if($return_as_string){
                            
                            return implode(',', $result);
                            
                        } else {
                            
                            return $result;   
                        }
                    }
                }
            }
        }
        
        return $values;
    }
        
    /**
     * Update items from the table
     *
     * @param string $table
     * @return boolean
     */

    public function update_database_table_data ($table) {

        $this->get_database_tables();

        $db_table  = $this->__verify_table_name($table);
        $sly_table = $this->table_prefix.$db_table;

        if (in_array($sly_table, $this->database_tables)) {

            $errors     = false;
            $conn_id    = $this->database_config['conn_id'];
            $modified   = date('Y-m-d H:i:s');
            $ids        = [];

            if (!$this->database_init_date) {
                
                $this->database_init_date = $modified;
            }

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

                if (is_array($schema) && !empty($schema) && is_array($data) && !empty($data)) {

                    $fields_conn = [];

                    foreach ($schema as $db_field => $info) {

                        if (!empty($info['has_multilingual'])) {

                            $fields_conn[$info['name'].'_'.$info['language']] = $db_field;

                        } else {

                            $fields_conn[$info['name']] = $db_field;
                        }
                    }

                    $field_id = $this->__get_field_key($db_table);

                    foreach ($data as $k =>& $register) {

                       $id     = addslashes($register['ID']);
                       $fields = [ $sly_table => "___modified='$modified'" ];

                       foreach ($register as $field =>& $f_data) {

                            if ($field == 'data') {

                                foreach ($f_data as $d_field =>& $value) {

                                    if (isset($fields_conn[$d_field]) && isset($schema[$fields_conn[$d_field]])) {

                                        $db_field = $fields_conn[$d_field];

                                        if (isset($this->column_tables[$sly_table][$db_field])) {

                                            $multi_db_table = $this->column_tables[$sly_table][$db_field];

                                            if               (!$multi_db_table)           $multi_db_table  = $sly_table;
                                            if (!isset($fields[$multi_db_table])) $fields[$multi_db_table] = '';

                                            $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."`{$fields_conn[$d_field]}`=";

                                            if (is_array($value)) {

                                                $fields[$multi_db_table] .= "'".addslashes($schema[$db_field]['type'] == 'list' ? implode(',' , $value) : json_encode($value))."'";

                                            } else {

                                                $fields[$multi_db_table] .= (($value === null or $value === '') ? 'null' : "'".addslashes($value)."'");
                                            }
                                        }
                                    }
                                }

                                unset($value);

                            } else if ($field != 'ID' and isset($fields_conn[$field])) {

                                if (is_array($f_data)) { $f_data = implode(',', $f_data); }

                                $db_field = $fields_conn[$field];

                                if (isset($this->column_tables[$sly_table][$db_field])) {

                                    $multi_db_table = $this->column_tables[$sly_table][$db_field]; 
                                    
                                    if               (!$multi_db_table)           $multi_db_table  = $sly_table;
                                    if (!isset($fields[$multi_db_table])) $fields[$multi_db_table] = '';

                                    $fields[$multi_db_table] .= ($fields[$multi_db_table] ? ', ' : '')."`{$fields_conn[$field]}` = '".addslashes($f_data)."'";
                                }
                            }

                            unset($register[$field]);
                        }

                        unset($register, $data[$k], $f_data);

                        if (!empty($fields)) {

                            $ok = true;
                     
                            if (isset($ids[$id])) {

                                $limit     = (count($fields) > 1 ? '' : ' limit 1');
                                $tables    =
                                $db_fields = '';

                                foreach ($fields as $multi_db_table => $string_fields) {

                                    $db_fields .= ($db_fields ? ', ' : '').$string_fields;
                             
                                    if (!isset($ids[$id][1][$conn_id]) &&  $multi_db_table == $sly_table) { 
                                        
                                        $db_fields .= ', `__conn_id__`=\''.addslashes($ids[$id][0].','.$conn_id).'\'';
                                    }

                                    if ($multi_db_table) {
                                        
                                        $tables .= ($tables ? " left join `$multi_db_table` on (`$multi_db_table`.`$field_id`=`$sly_table`.`$field_id`)" 
                                                              : 
                                                              "`$multi_db_table`");
                                    }
                                }

                                $SQL = "update $tables set $db_fields where `$sly_table`.`$field_id`='$id'$limit;";

                                if (!$this->DB->execute($this->__add_to_debug($SQL))) $ok = false;

                            } else {

                                foreach ($fields as $multi_db_table => $string_fields) {

                                    if ($multi_db_table == $sly_table) $string_fields .= ", `__conn_id__`='$conn_id'";

                                    $SQL = "insert into `$multi_db_table` set `$field_id`='$id', $string_fields;";

                                    if (!$this->DB->execute($this->__add_to_debug($SQL)) && $this->DB->error) $ok = false;
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

                        if (!empty($ids[$id][1])) {

                            $SQL = "update `$sly_table` set `__conn_id__`='".addslashes(implode(',', $ids[$id][1]))."' where `$field_id`='$id' limit 1;";

                            if (!$this->DB->execute($this->__add_to_debug($SQL))) {

                                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                                $errors = true;
                            }
                        }

                        unset($ids_deleted[$k], $ids[$id]);
                    }
                }

                if ($num_deletes = count($ids_deleted)) {

                    foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                        $SQL = "delete from `$multi_db_table` where `$field_id` IN ('".implode("','", $ids_deleted)."') limit $num_deletes;";

                        if (!$this->DB->execute($this->__add_to_debug($SQL)) && $multi_db_table == $sly_table) {

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
     * Remove outdated items from the table
     *
     * @param string $table
     * @return boolean
     */

    public function clean_database_table_updated ($table) {

        if ($this->database_init_date) {

            $this->get_database_tables();

            $errors    = false;
            $db_table  = $this->__verify_table_name($table);
            $sly_table = $this->table_prefix.$db_table;

            foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                $SQL = "delete from `$multi_db_table` where `___modified`<'{$this->database_init_date}'".
                       (count($this->get_connectors_list()) > 1 ? " and find_in_set('".$this->database_config['conn_id']."', `__conn_id__`)" : '').';';

                if (!$this->DB->execute($this->__add_to_debug($SQL)) && $multi_db_table == $sly_table) {

                    if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                    $errors = true;
                }
            }

            if (!$errors) return true;
        }

        return false;
    }

    /**
     * Clean all data of table
     *
     * @param $table string
     * @return boolean
     */

    public function clean_database_table_data ($table) {

        $this->get_database_tables();

        $db_table  = $this->__verify_table_name($table);
        $sly_table = $this->table_prefix.$db_table;

        if (in_array($sly_table, $this->database_tables)) {

            $this->get_database_table_fields($sly_table);

            foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                $SQL = "TRUNCATE TABLE `$multi_db_table`;";

                if (!$this->DB->execute($this->__add_to_debug($SQL)) && $multi_db_table == $sly_table) {

                    if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                    $errors = true;
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
     * @param $group array list order data
     * @param $limit array list limit data: [ 'page' => number, 'limit' => number ]
     * @param $get_internal_ids bool
     * @param $get_internal_names bool
     * @param $get_channel_id bool
     * @param $join_child_tables bool
     * @param $test_child_tables bool
     * @return array
     */

    public function extract (
        
        $table, 
        $fields                 = null, 
        $language               = null, 
        $conditions             = null, 
        $force_default_language = null, 
        $order                  = null,
        $group                  = null,
        $limit                  = null,
        $get_internal_ids       = false, 
        $get_internal_names     = false, 
        $get_channel_id         = false,
        $join_child_tables      = false,
        $test_child_tables      = false

        ) {

        if ($table && $this->table_schema_exists($table)) {

            $this->get_database_tables();

            $db_table  = $this->__verify_table_name($table); 
            $sly_table = $this->table_prefix.$db_table;

            if (in_array($sly_table, $this->database_tables)) {

                $language        = $this->__test_language($language);
                $base_language   = $this->get_default_language();
                $all_fields      = true;
                $fields_excluded = [];

                if ($force_default_language === null)                       { $force_default_language = $this->need_autocomplete_language(); }
                if ($force_default_language && $language == $base_language) { $force_default_language = false; }

                if (is_array($fields) && !empty($fields)) {

                    if (isset($fields['exclude']) && is_array($fields['exclude'])) {

                        $fields_excluded = $fields['exclude'];

                    } else {

                        $all_fields = false;
                    }
                }

                $select             =
                $field_title        =
                $where              =
                $sql_group          =
                $sql_order          = 
                $field_id_db        =
                $field_id_name      = '';
                $tables_db          =
                $concats            =
                $table_fields       =
                $field_types        =
                $select_list        =
                $sub_wheres         =
                $sub_where_tables   =
                $field_keys_added   = [];
                $table_count        = 
                $field_count        = 0;

                if (isset($group['concat-fields'])) {

                    $concats = (array)$group['concat-fields'];
                    $group   = (isset($group['group-fields']) ? $group['group-fields'] : null);
                }

                $add_joins       = ($join_child_tables || $test_child_tables);
                $table_joins     = $this->__get_table_joins($table, $add_joins);
                $have_conditions = (empty($conditions) ? false : true);
                $have_group      = (empty($group)      ? false : true);

                foreach ($table_joins as $join_table => $join_field_id) {

                    $table_count ++;
                    $fields_added  = false;
                    $schema        = $this->get_database_table_schema($join_table, false);
                    $schema_names  = $this->__get_schema_names       ($join_table, $schema);

                    if ($all_fields) {

                        $fields = [];

                        if ($join_child_tables || $join_table == $table) {

                            $fields = array_keys($schema);

                            if (!$get_internal_ids) {

                                foreach ($fields as $k => $field) {

                                    if (substr($field, 0, 3) === '___') unset($fields[$k]);
                                }
                            }
                        }
                    }

                    $field_names = [];

                    foreach ($fields as $name => $field) {

                        $field = $this->__exists_field_in_schema($field, $schema, $schema_names);

                        if ($field && !in_array($field, $fields_excluded)) {

                            $info       =& $schema[$field];
                            $field_name =  addslashes(is_string($name) ? $name : ($get_internal_names ? $field : $info['name']));

                            if (   (!$join_child_tables && isset($field_names[$field_name])) 
                                || ( $join_child_tables && $info['type'] == 'key' && isset($field_keys_added[$field]))) continue;

                            $is_file      =  in_array($info['type'], [ 'image', 'file' ]);
                            $multi        = ((isset($info['has_multilingual']) && $info['has_multilingual']));
                            $fields_added ++;
    
                            if ($force_default_language && $multi) {

                                $db_field           = $field.'_'.$language;
                                $db_field_base      = $field.'_'.$base_language;
                                $this_db_table      = $this->__get_table_for_field($db_field,      $join_table);
                                $this_db_table_base = $this->__get_table_for_field($db_field_base, $join_table);

                                if ($this_db_table and $this_db_table_base) {

                                    $select_field = "IF(`$this_db_table`.`$db_field`!=''".($is_file ? " and `$this_db_table`.`$db_field`!='[]'" : '').
                                                    ", `$this_db_table`.`$db_field`, `$this_db_table_base`.`$db_field_base`)";

                                    if (!isset($tables_db[$this_db_table])) {
                                        
                                        $tables_db[$this_db_table] = $this->__get_field_id_for_table_join($this_db_table, $join_field_id);
                                    }

                                    if ($this_db_table != $this_db_table_base && !isset($tables_db[$this_db_table_base])) {
                                        
                                        $tables_db[$this_db_table_base] = $this->__get_field_id_for_table_join($this_db_table_base, $join_field_id);
                                    }
                                }

                            } else {

                                $db_field      = $field.($multi ? '_'.$language : '');
                                $this_db_table = $this->__get_table_for_field($db_field, $join_table);

                                if ($this_db_table) {

                                    $select_field  = "`$this_db_table`.`$db_field`";

                                    if (!isset($tables_db[$this_db_table])) {
                                        
                                        $tables_db[$this_db_table] = $this->__get_field_id_for_table_join($this_db_table, $join_field_id);
                                    }
                                }
                            }

                            if (isset($concats[$field])) {

                                $separator    = ((isset($concats[$field]['separator']) and $concats[$field]['separator']) ? $concats[$field]['separator'] : ', ');
                                $select_field = 'GROUP_CONCAT('.$select_field.
                                                ((isset($concats[$field]['order']) and $concats[$field]['order']) ? 
                                                    ' ORDER BY 1 '. (strtolower(substr($concats[$field]['order'], 0, 1)) != 'd' ? 'ASC' : 'DES') : '').
                                                ' SEPARATOR \''.addslashes($separator).'\')';
                            }

                            $field_ref = 't'.$table_count.'f'.($field_count ++).'.'.$field;

                            if ($info['type'] == 'key' && $info['name'] == 'ID') {

                                $field_keys_added[$field] = $field_name;
                            }

                            $select_list [$field_ref]  = $select_field.' as \''.addslashes($field_ref).'\'';
                            $field_types [$field_ref]  = $info['type'];
                            $table_fields[$field_ref]  = [ $join_table, $field, $field_name ];
                            $field_names [$field_name] = $field_ref;

                            unset($info);

                            if ($join_table == $table && preg_match('/^\w+_(title|name)(_.*)?$/', $field)) { $field_title = $field; }
                        }
                    }

                    if ($fields_added) {

                        $field_id_db   = "`$this_db_table`.`$join_field_id`";
                        $field_id_name = (isset($schema[$join_field_id]) ? ($get_internal_names ? $join_field_id : addslashes($schema[$join_field_id]['name'])) : '__id__');
                    }

                    if ($have_conditions) {

                        list($sub_where, $tables_db_where) = $this->__get_where_for_extract($conditions, $schema, $join_table, $force_default_language, $language, $base_language, '', 'or');

                        if ($sub_where and !empty($tables_db_where)) { 
 
                            $sub_table = array_shift($tables_db_where);

                            if ($sub_table !== $sly_table) {

                                $sub_wheres[$sub_table]       = $sub_where;
                                $sub_where_tables[$sub_table] = [$join_field_id, $tables_db_where];

                            } else {

                                $where .= ($where ? ' or ' : '').$sub_where;
                            }
                        }
                    }

                    if ($have_group) {

                        list($part_group, $tables_db_group) = $this->__get_group_for_extract($group, $schema, $join_table, $language, $base_language);

                        if ($part_group) {

                            $sql_group .= ($sql_group ? ', ' : '').$part_group;
                            
                            foreach ($tables_db_group as $table_group) { 
                                
                                if (!isset($tables_db[$table_group])) {
                                    
                                    $tables_db[$table_group] = $this->__get_field_id_for_table_join($table_group, $join_field_id);
                                }
                            }
                        }
                    }

                    if (is_array($order) and !empty($order)) {

                        foreach ($order as $field => $ord) {

                            $field = $this->__exists_field_in_schema($field, $schema, $schema_names);

                            if ($field) {

                                unset($order[$field]);

                                if (!$db_field = $this->__get_real_field($field, $join_table, $language)) {
                                     $db_field = $this->__get_real_field($field, $join_table, $base_language);
                                }

                                if ($db_field) {

                                    if (strtoupper($ord) != 'ASC') { $ord = 'DESC'; }

                                    $this_db_table = $this->__get_table_for_field($db_field, $join_table);

                                    if ($this_db_table) {

                                        $sql_order .= ($sql_order ? ', ' : '')."`$this_db_table`.`$db_field` $ord";

                                        if (!isset($tables_db[$this_db_table])) {
                                            
                                            $tables_db[$this_db_table] = $this->__get_field_id_for_table_join($this_db_table, $join_field_id);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if (!empty($select_list)) {

                    $select = implode(', ', $select_list); 

                    if ($field_title and !$sql_order) {

                        if (!$db_field = $this->__get_real_field($field_title, $table, $language) and $language != $base_language) {
                             $db_field = $this->__get_real_field($field_title, $table, $base_language);
                        }

                        if ($db_field) { 
                            
                            $this_db_table = $this->__get_table_for_field($db_field, $table);

                            if ($this_db_table) {

                                $sql_order = "`$this_db_table`.`$db_field` ASC";
                            
                                if (!isset($tables_db[$this_db_table])) {
                                    
                                    $tables_db[$this_db_table] = $this->__get_field_id_for_table_join($this_db_table, $table_joins[$table]);
                                }
                            }
                        }   
                    }

                    $string_tables = '';

                    foreach ($tables_db as $this_db_table => $this_db_join_id) {

                        if ($this_db_table) {
                            
                            $string_tables .= ($string_tables ? " left join `$this_db_table` on (`$this_db_table`.`$this_db_join_id`=`$sly_table`.`$this_db_join_id`)" 
                                                                : 
                                                                "`$this_db_table`");
                        }
                    }

                    if ($string_tables) {

                        if (!empty($sub_where_tables)) {

                            $sub_where_tables[$sly_table] = [$tables_db[$sly_table], []];
           
                            foreach ($sub_where_tables as $this_db_table => $this_db_config) {

                                $sub_where = $this->__add_sub_selects_in_where($sly_table, $this_db_table, $this_db_config[0], $sub_where_tables, $sub_wheres);

                                if ($sub_where) $where .= ($where ? ' or ' : '').$sub_where;
                            }
                        }

                        $SQL = 'select '.(($get_internal_ids and $field_id_name) ? "$field_id_db as `$field_id_name`, " : '').
                                         ($get_channel_id ? "`$this_db_table`.`__conn_id__`, " : '')."$select from $string_tables".
                                         ($where ? ' where '.$where : '').
                                         ($sql_group ? ' group by '.$sql_group : '').
                                         ($sql_order ? ' order by '.$sql_order : '').
                                         ((is_array($limit) and isset($limit['limit']) and $limit['limit']) ? 
                                            ' limit '.($limit['page'] > 0 ? addslashes($limit['page']).', ' : '').addslashes($limit['limit']) 
                                            : 
                                            (is_numeric($limit) ? ' limit 0, '.addslashes($limit) : ''));

                        $res = $this->DB->execute($this->__add_to_debug($SQL));

                        if (!is_array($res)) {

                            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);

                            return false;
                        }
            
                        if (is_array($res) && !empty($res)) {

                            if (!isset($res[0])) { $res = array($res); }

                            $list = [];

                            foreach ($res as $k =>& $data) {

                                $list[$k] = [];

                                foreach ($data as $field => $value) {

                                    if (isset($field_types[$field])) {

                                        list($table_join, $real_field, $public_field) = $table_fields[$field];

                                        if (!($exists = array_key_exists($public_field, $list[$k])) or !empty($value)) {

                                            if (in_array($field_types[$field], [ 'image', 'file' ])) {

                                                $value = (!in_array($value, ['[]', '{}']) ? json_decode($value, 1) : '');

                                                if (!$exists || !empty($value)) $list[$k][$public_field] = $value;

                                            } else if ($field_types[$field] == 'list') {

                                                $value = $this->get_list_field_values_translations($language, $real_field, $table_join, $value);

                                                if (!$exists || !empty($value)) $list[$k][$public_field] = $value;

                                            } else {

                                                $list[$k][$public_field] = $value;
                                            }
                                        }

                                    } else if (!array_key_exists($field, $list[$k])) {

                                        $list[$k][$field] = $value;
                                    }
                                }
                                unset($res[$k]);
                            }
      
                            return $list;
                        }
                    }
                }

            } else $this->__trigger_error('Uknow database table: '.$sly_table, 107);
        }     else $this->__trigger_error('Uknow table: '.$table, 106);

        return [];
    }

    /**
     * Get the total number of rows of extract with conditions
     *
     * @param $table string database table
     * @param $language string language need
     * @param $conditions array for where
     * @param $force_default_language boolean include default language info
     * @param $group array list order data
     * @param $test_child_tables bool
     *
     * @return integer
     */

    public function get_num_rows (
        
        $table, 
        $language               = null, 
        $conditions             = null, 
        $force_default_language = false, 
        $group                  = null,
        $test_child_tables      = false
    
        ) {

        $this->get_database_tables();

        $db_table  = $this->__verify_table_name($table); 
        $sly_table = $this->table_prefix.$db_table;

        if (in_array($sly_table, $this->database_tables) && $this->table_schema_exists($table)) {

            if (isset($group['concat-fields'])) {

                $group = (isset($group['group-fields']) ? $group['group-fields'] : null);
            }

            $language       = $this->__test_language($language);
            $base_language  = $this->get_default_language();
            $tables_db      = 
            $tables_where   =
            $sub_wheres     = [];
            $sql_group      = '';

            if ($force_default_language && $language == $base_language) { $force_default_language = false; }

            $have_conditions = (empty($conditions) ? false : true);
            $have_group      = (empty($group)      ? false : true);
            $table_joins     = $this->__get_table_joins($table, $test_child_tables);

            foreach ($table_joins as $join_table => $join_field_id) {

                $schema = $this->get_database_table_schema($join_table, false);

                if ($test_child_tables and $join_table != $table) {

                    $db_join_table = $this->table_prefix.$this->__verify_table_name($join_table); 

                    if (!isset($tables_db[$db_join_table])) { $tables_db[$db_join_table] = [$join_field_id, [$db_join_table]]; }
                }

                if ($have_conditions) {

                    list($sub_where, $tables_db_where) = $this->__get_where_for_extract($conditions, $schema, $join_table, $force_default_language, $language, $base_language, '', 'or');

                    if  ($sub_where) {

                        $sub_table              = array_shift($tables_db_where);
                        $sub_wheres[$sub_table] = $sub_where;

                        if (!isset($tables_where[$sub_table])) { $tables_where[$sub_table]    = [$join_field_id, []]; }
                        if (!empty($tables_where))             { $tables_where[$sub_table][1] = array_merge($tables_where[$sub_table][1], $tables_db_where); }
                    }
                }

                if ($have_group) {

                    list($part_group, $tables_db_group) = $this->__get_group_for_extract($group, $schema, $join_table, $language, $base_language);

                    if  ($part_group) {

                        $sql_group .= ($sql_group ? ', ' : '').$part_group;
                        $sub_table  = array_shift($tables_db_group);

                        if (!isset($tables_db[$sub_table])) { $tables_db[$sub_table]    = [$join_field_id, []]; }
                        if (!empty($tables_db_where))       { $tables_db[$sub_table][1] = array_merge($tables_db[$sub_table][1], $tables_db_group); }
                    }
                }
            }

            $SQL = "select SQL_CACHE count(1) as total from `$sly_table`";

            if (  !empty($tables_db)) {
            
                foreach ($tables_db as $config) {

                    foreach ($config[1] as $join_table) {

                        if ($join_table != $sly_table) {

                            $SQL .= " left join `$join_table` on (`$join_table`.`{$config[0]}`=`$sly_table`.`{$config[0]}`)";
                        }
                    }
                }
            }

            $where = (isset($sub_wheres[$sly_table]) ? $sub_wheres[$sly_table] : '');
            
            if (!isset($tables_where[$sly_table])) { $tables_where[$sly_table] = [$join_field_id, [$sly_table]]; }

            if (count($tables_where) > 1) {
    
                foreach ($tables_where as $this_db_table => $this_db_config) {

                    $sub_where = $this->__add_sub_selects_in_where($sly_table, $this_db_table, $this_db_config[0], $tables_where, $sub_wheres);

                    if ($sub_where) $where .= ($where ? ' or ' : '').$sub_where;
                }
            }

            if ($where) $SQL .= ' where '.$where;

            if ($sql_group) {

                $SQL = 'select SQL_CACHE count(1) as total from ('.preg_replace('/^(select\s+)SQL_CACHE\s+/i', '\\1', $SQL).' group by '.$sql_group.') as q';
            }

            $res = $this->DB->execute($this->__add_to_debug($SQL));

            if (isset($res[0])) {

                return $res[0]['total'];

            } else if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
        }

        return 0;
    }

    /**
     * Get real field for table join
     * 
     * @return string
     */

    function __get_field_id_for_table_join ($table_join, $join_field_id) {

        if (preg_match('/___[0-9]+$/', $table_join)) {
            
            $table_base    = preg_replace(['/^'.preg_quote($this->table_prefix, '/').'/', '/___[0-9]+$/'], '', $table_join);
            $join_field_id = $this->__get_field_key($table_base);
        }

        return $join_field_id;
    }

    /**
     *  Get sub-selects in where
     *
     * @return string
     */

    function __add_sub_selects_in_where ($sly_table, $db_table, $db_id, &$tables_db, &$sub_wheres) {

        $SQL = '';

        foreach ($tables_db as $this_db_table => $this_db_config) {

            if ($this_db_table != $db_table and $this_db_table != $sly_table and $this_db_config[0] == $db_id) {

                $SQL .= ($SQL ? ' or ' : '')."`$sly_table`.`{$this_db_config[0]}` IN (select DISTINCT `{$this_db_config[0]}` from `$this_db_table`";

                if (  !empty($this_db_config[1])) {
            
                    foreach ($this_db_config[1] as $join_table) {

                        $SQL .= " left join `$join_table` on (`$join_table`.`{$this_db_config[0]}`=`$sly_table`.`{$this_db_config[0]}`)";
                    }
                }

                if (isset($sub_wheres[$this_db_table])) {

                    $SQL .= ' where '.$sub_wheres[$this_db_table].') and '.(substr($sub_wheres[$this_db_table], 0, 1) == '(' ? 
                                                                                   $sub_wheres[$this_db_table] : '('.$sub_wheres[$this_db_table].')');
                } else {

                    $SQL .= ')';
                }
            }
        }

        return $SQL;
    }

    /**
     *  Get tables in join
     *
     * @param $table string database table
     * @param $join_child_tables bool
     *
     * @return array
     */
    
    private function __get_table_joins ($table, $join_child_tables = false) {

        $db_table    = $this->__verify_table_name($table);
        $table_joins = [ $table => $this->__get_field_key($db_table) ];

        if ($join_child_tables) {

            $tables = $this->get_all_table_joins();

            foreach ($tables as $test_table => $joins) {

                if ($test_table != $table) {

                    foreach ($joins as $join_table => $join_key) {

                        if ($join_table == $table) $table_joins[$test_table] = $this->__get_db_key_from_field($join_key, $join_table);
                    }
                }
            }
        }

        return $table_joins;
    }

    /**
     * Get schema field names
     *
     * @param $table string database table
     * @param $schema array
     *
     * @return array
     */

    private function __get_schema_names ($table, &$schema) {

        $schema_names = []; 

        foreach ($schema as $field =>& $info) { $schema_names[$info['name']] = $field; } unset($info);

        return $schema_names;
    }

    /**
     * Get where conditions for export
     *
     * @param $conditions array
     * @param $schema array
     * @param $table string database table
     * @param $force_default_language string default language code
     * @param $language string language code
     * @param $base_language string base language code
     * @param $where string existing where
     * @param $default_logic string default logic union
    *
     * @return array
     */

    private function __get_where_for_extract ($conditions, &$schema, $table, $force_default_language, $language, $base_language, $where = '', $default_logic = 'and') {

        $tables_db = [];
        
        if (is_array($conditions) && !empty($conditions)) {

            $group_open   = 0;
            $new_where    = '';
            $schema_names = $this->__get_schema_names($table, $schema);

            foreach ($conditions as &$param) {

                if (isset($param['group'])) {

                    if ($param['group'] == 'close') {

                        if ($group_open) $new_where .= ')'; else -- $group_open;

                    } else {

                        $new_where .= ' '.($new_where ? (in_array($param['group'], ['or', 'not', 'xor']) ? $param['group'] : $default_logic).' ' : '').' (';

                        ++ $group_open;
                    }

                } else if (isset($param['field'])) {

                    $clause = '';

                    if  (isset($param['search']) && $param['search']) {

                        $sfields = (is_array($param['field']) ? $param['field'] : explode(',', $param['field']));
                        $sfields = array_unique($sfields);
                        $fgroup  = '';
                   
                        foreach ($sfields as $field) {

                            if ($field) {
    
                                $field = $this->__exists_field_in_schema($field, $schema, $schema_names);
    
                                if ($field) {

                                    if (!$db_field = $this->__get_real_field($field, $table, $language)) {
                                         $db_field = $this->__get_real_field($field, $table, $base_language);
                                    }

                                    if ($db_field) { 
                                        
                                        $this_db_table = $this->__get_table_for_field($db_field, $table);

                                        if ($this_db_table) {

                                            $fgroup .= ($fgroup ? ', ' : '')."COALESCE(`$this_db_table`.`$db_field`,'' COLLATE ". $this->__get_collate().')';

                                            if (!in_array($this_db_table, $tables_db)) $tables_db[] = $this_db_table;
                                        }

                                        if (   $force_default_language 
                                            && $language != $base_language
                                            && isset($schema[$field]['has_multilingual'])
                                            &&       $schema[$field]['has_multilingual']
                                            && $db_field = $this->__get_real_field($field, $table, $base_language)) {

                                            $this_db_table = $this->__get_table_for_field($db_field, $table);

                                            if ($this_db_table) {

                                                $fgroup .= ($fgroup ? ', ' : '')."COALESCE(`$this_db_table`.`$db_field`,'' COLLATE ". $this->__get_collate().')';

                                                if (!in_array($this_db_table, $tables_db)) $tables_db[] = $this_db_table;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if ($fgroup) {

                            $clause = ((isset($param['strict']) && $param['strict']) ? 'BINARY ' : '').
                                      'lower('.(count($sfields) > 1 ? "concat($fgroup)" : $fgroup).") like '%".addslashes(strtolower($param['search']))."%'";
                        }

                    } else if (isset($param['value']) && $db_field = $this->__get_real_field($param['field'], $table, $language)) {

                        if (!isset($param['condition']) || !$param['condition']) $param['condition'] = '=';

                        $filter = (is_array($param['value']) ? ($param['condition'] != '=' ? ' NOT' : '')." IN ('".implode("','", array_map('addslashes', $param['value']))."')"
                                                                :
                                                                $param['condition']."'".addslashes($param['value'])."'");

                        $this_db_table = $this->__get_table_for_field($db_field, $table);

                        if ($this_db_table) {

                            $clause = "(`$this_db_table`.`$db_field`$filter".($param['strict'] ? " and BINARY `$this_db_table`.`$db_field`$filter" : '').')';

                            if (!in_array($this_db_table, $tables_db)) $tables_db[] = $this_db_table;
                        }

                        if (   $force_default_language
                            && $language != $base_language
                            && isset($schema[$param['field']]['has_multilingual'])
                            &&       $schema[$param['field']]['has_multilingual']
                            && $db_field = $this->__get_real_field($param['field'], $table, $base_language)) {

                            $this_db_table = $this->__get_table_for_field($db_field, $table);

                            if ($this_db_table) {

                                
                                $clause = "($clause or (`$this_db_table`.`$db_field`$filter".($param['strict'] ? " and BINARY `$this_db_table`.`$db_field`$filter" : '').'))';

                                if (!in_array($this_db_table, $tables_db)) $tables_db[] = $this_db_table;
                            }
                        }
                    }

                    if ($clause) {

                        $new_where .= (($new_where && substr($new_where, -1) != '(') ? ' '.((isset($param['logic']) and $param['logic']) ? $param['logic'] : $default_logic).' ' : '').$clause;
                    }
                }
            }
            unset($param);

            if ($new_where) {
                        
                $where = ($where ? (substr($where, 0, 1) != '(' ? "($where)" : $where)." or ($new_where)" : $new_where);
            }

            if (count($tables_db) > 1) $tables_db = array_unique($tables_db);
        }

        return [ $where, $tables_db ];
    }

    /**
     * Get SQL group for export
     *
     * @param $group array
     * @param $schema array
     * @param $table string database table
     * @param $language string language code
     * @param $base_language string base language code
     *
     * @return string
     */

     private function __get_group_for_extract (&$group, &$schema, $table, $language, $base_language) {

        $sql_group      = '';
        $this_db_tables = [];

        if ($group !== null and !empty($group)) {

            $schema_names = $this->__get_schema_names($table, $schema);

            if (!is_array($group)) $group = [ $group ];
     
            foreach ($group as $k => $field) {

                $field = $this->__exists_field_in_schema($field, $schema, $schema_names);

                if ($field) {

                    if (!$db_field = $this->__get_real_field($field, $table, $language)) {
                         $db_field = $this->__get_real_field($field, $table, $base_language);
                    }

                    if ($db_field) {

                        $this_db_table = $this->__get_table_for_field($db_field, $table);

                        if ($this_db_table) {

                            $this_db_tables[] = $this_db_table;
                            $sql_group       .= ($sql_group ? ', ' : '')."BINARY `$this_db_table`.`$db_field`";
                        }
                    }

                    unset($group[$k]);
                }
            }
        }

        return [$sql_group, $this_db_tables];
    }

    /**
     * Get field if exists in schema
     *
     */

    private function __exists_field_in_schema ($field, &$schema, &$schema_names) {

        if (!isset($schema[$field])) {

            if ( isset($schema_names[$field])) {
                
                return $schema_names[$field];
              
            } else if (($strlow_field = strtolower($field)) !== $field) {

                if (isset($schema[$strlow_field])) {

                    return $strlow_field;
                }
            } 

            return false;
        }

        return $field;
    }

    /** 
     *  Gets a multi-table name from a field
     *
     * @param $db_field string database column name
     * @param $table string database table
     */

    private function __get_table_for_field ($db_field, $table) {

        if ($table) {

            $db_table  = $this->__verify_table_name($table);
            $sly_table = $this->table_prefix.$db_table;

            if (!isset($this->column_tables[$sly_table])) $this->get_database_table_fields($sly_table);

            if ( isset($this->column_tables[$sly_table][$db_field])) {

                return $this->column_tables[$sly_table][$db_field];
            }

            return '';
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

        if (!empty($this->database_tables)) {

            $tables = (!empty($this->database_config['data_schema']) ? array_keys($this->database_config['data_schema']) : []);

            if (!empty($tables)) {

                if ($delete_config == true) { $tables[] = $this->table_config; }

                foreach ($tables as $table) {

                    $sly_table = $this->table_prefix.$table;

                    if (in_array($sly_table, $this->database_tables)) {

                        $SQL = "DROP TABLE IF EXISTS `$sly_table`";

                        if (!$this->DB->execute($this->__add_to_debug($SQL))) {

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

            if (!$this->DB->execute($this->__add_to_debug($SQL))) {

                if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
            }

            if ($clean_items && $this->response_error!=104) {

                $tables = (!empty($this->database_config['data_schema']) ? array_keys($this->database_config['data_schema']) : []);

                if (!empty($tables)) {

                    $conn_id = $this->database_config['conn_id'];

                    foreach ($tables as $table) {

                        $db_table  = $this->__verify_table_name($table);
                        $sly_table = $this->table_prefix.$db_table;
                        $ids       = [];

                        foreach ($this->get_database_table_ids($table, true) as $v) {

                            $ids[$v['id']] = array_flip(explode(',', $v['conn_id']));
                        }

                        $del_ids[$sly_table] = array_keys($ids);

                        if (!empty($ids)) {

                            $field_id = $this->__get_field_key($db_table);

                            foreach ($ids as $id => $cons) {

                                if (count($cons) > 1 && isset($cons[$conn_id])) {

                                    unset($cons[$conn_id]);

                                    if (!empty($cons)) {

                                        $SQL = "update `$sly_table` set `__conn_id__`='".addslashes(implode(',', array_flip($cons)))."' where `$field_id`='$id' limit 1;";

                                        if (!$this->DB->execute($this->__add_to_debug($SQL))) {

                                            if ($this->DB->error) $this->__trigger_error($this->DB->error." ($SQL)", 104);
                                        }
                                    }

                                    unset($ids[$id]);
                                }
                            }

                            if (!empty($ids)) {

                                $this->get_database_table_fields($sly_table);

                                $where = '`'.$field_id.'` IN ('.implode(', ', array_keys($ids)).')';

                                foreach ($this->rel_multitables[$sly_table] as $multi_db_table) {

                                    $SQL = "delete from `$multi_db_table` where $where;";

                                    if (!$this->DB->execute($this->__add_to_debug($SQL))) {

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
     * Get headings of fields in certain language
     *
     * @param string $language (ISO 639-1)
     *
     * @return array
     */

     public function get_headings ($language, $table = null) {

        $this->__test_config_initialized();

        if (!isset($this->database_config['conn_schema']['headings'])) { return []; };

        $headings         = [];
        $default_language = $this->get_default_language();
   
        if ($table) {

            if (   isset($this->database_config['conn_schema']['headings'][$table])) {

                foreach ($this->database_config['conn_schema']['headings'][$table] as $info) {

                    $title = $info['title'];

                    if (isset($info['titles'])) {
                        
                        if      (isset($info['titles'][$language])         and $info['titles'][$language])         { $title = $info['titles'][$language]; }
                        else if (isset($info['titles'][$default_language]) and $info['titles'][$default_language]) { $title = $info['titles'][$default_language]; }
                    }

                    $headings[] = [

                        'title'          => $title,
                        'position'       => $info['position'],
                        'field_previous' => $info['field_previous']
                    ];
                }
                unset($info_fields);
            }

        } else {

            foreach ($this->database_config['conn_schema']['headings'] as $table =>& $titles) {

                $headings[$table] =  [];

                foreach ($titles as $info) {

                    $title = $info['title'];

                    if (isset($info['titles'])) {
                        
                        if      (isset($info['titles'][$language])         and $info['titles'][$language])         { $title = $info['titles'][$language]; }
                        else if (isset($info['titles'][$default_language]) and $info['titles'][$default_language]) { $title = $info['titles'][$default_language]; }
                    }

                    $headings[$table][] = [

                        'title'          => $title,
                        'position'       => $info['position'],
                        'field_previous' => $info['field_previous']
                    ];
                }
                unset($info_fields);
            }
            unset($titles);
        }
        
        return $headings;

     }

     /**
     * Get custom parameter
     *
     * @return value
     */

    function get_custom_paremeter ($param) {

        $this->__test_config_initialized();

        return (    isset($this->database_config['conn_schema']['custom_parameters']) 
                and isset($this->database_config['conn_schema']['custom_parameters'][$param]) ? $this->database_config['conn_schema']['custom_parameters'][$param] : null);
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
     * Set debug
     *
     * @param $active boolean
     *
     */

    public function set_debug ($active) {

        $this->debug = ($active !== false ? ($active == 'file' ? 'file' : true) : false);
    }

    /**
     * Print information debuged
     *
     */

    public function print_debug () {

        if ($this->debug !== false and !empty($this->SQL_list)) {

            $s = "\n\n[SLYR_Updater] List of SQL's at ".date('Y-m-d H:i:s').
                 ' ('.sprintf("%05.2f", (memory_get_usage(true) / 1024) / 1024)."MBytes RAM used):\n".
                 print_r($this->SQL_list, 1)."\r\n";

            if      ($this->debug === 'file') { file_put_contents($this->debbug_file_name, $s, FILE_APPEND); }
            else if ($this->debug !== 'var')  { echo $s; flush(); ob_flush(); }

            $this->SQL_list = [];

            return $s;
        }

        return '';
    }

    /**
     * Add trace to debbug
     *
     */

    private function __add_to_debug ($trace, $is_error = false) {

        if ($this->debug !== false) {

            $this->SQL_list[] = $trace;

            if ($is_error) { $this->SQL_errors[] = key($this->SQL_list); }

            if ($this->debug_max_list and count($this->SQL_list) > $this->debug_max_list) { $this->print_debug(); }
        }

        return $trace;
    }

    /**
     * Get number of SQL errors
     *
     */

     public function get_SQL_errors_num () {

        return count($this->SQL_errors);
     }

     /**
     * Get list of SQL errors
     *
     */

    public function get_SQL_errors_list () {

        $list = [];

        if (!empty($this->SQL_errors)) {

            if (!empty($this->SQL_list)) {

                foreach ($this->SQL_errors as $key) {

                    $list[] = $this->SQL_list[$key];
                }
                
            } else {

                return $this->SQL_errors;
            }
        }

        return $list;
     }

    /**
     * Get path to save the logs
     *
     */

    public function get_path_log_debug () {

        if (!isset($this->debug_file_path)) {

            $this->debug_file_path = dirname(__FILE__).DIRECTORY_SEPARATOR;
        }

        return $this->debug_file_path;
    }

    /**
     * Set path to save the logs
     *
     */

    public function set_path_log_debug ($path = '') {

        $this->debug_file_path  = $path;
        $this->debbug_file_name = $this->get_path_log_debug().$this->debug_file_prefix.date('_Y-m-d_H-i').'.log';
    }

    /**
     * Get path and filename to save the logs
     *
     */

     public function get_filename_log_debug () {

        return $this->debbug_file_name;
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

        $field = strtolower(preg_replace(['/[^a-z0-9_\-]+/i', '/_{2,}/'], '_', $field));

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

         if ($errnum == 104) { $this->__add_to_debug("ERROR $errnum: $message", true); }

         parent::__trigger_error($message, $errnum);
     }

     /**
      * Clean cache
      *
      */

    public function clean_cache () {

        $this->response_error    = false;
        $this->database_config   = 
        $this->list_connectors   = 
        $this->SQL_errors        = [];
        $this->database_tables   =
        $this->test_update_stats = null;

        $this->clean_table_cache();
    }
}

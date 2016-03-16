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
 * @modified 2016-03-16
 * @version 1.11
 *
 */

if                                (!class_exists('SalesLayer_Conn')) include_once dirname(__FILE__).DIRECTORY_SEPARATOR.'SalesLayer-Conn.php';
if (extension_loaded('PDO')) { if (!class_exists('slyr_SQL'))        include_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.DBPDO.php'; }
else if                           (!class_exists('slyr_SQL'))        include_once dirname(__FILE__).DIRECTORY_SEPARATOR.'lib/class.MySQL.php';

class SalesLayer_Updater extends SalesLayer_Conn {

    public  static $database = null;
    public  static $username = null;
    public  static $password = null;
    public  static $hostname = null;

    public  static $table_prefix = 'slyr_';
    public  static $table_config = '__api_config';

    public  static $list_connectors = array();

    public  static $DB       = null;
    public  static $SQL_list = array();

    public  static $debbug   = false; // <-- false / true / 'file'

    private static $database_tables = null;
    private static $database_fields = array();
    private static $database_config = array();

    private static $updater_version = '1.11';
    private static $api_version     = '1.17';

    private static $database_field_types = array(

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

    private static $database_field_types_charset = array(

        'text'      =>'CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL',
        'mediumtext'=>'CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL',
        'bool'      =>'NOT NULL',
        'float'     =>'NOT NULL',
        'datetime'  =>'NOT NULL',
        'int'       =>'NOT NULL'
    );

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

    public function __construct ($database=null, $username=null, $password=null, $hostname=null, $codeConn=null, $secretKey=null, $SSL=false, $url=false) {

        if (self::__has_system_requirements() && $database!=null) {

               self::connect($database, $username, $password, $hostname, $codeConn, $secretKey, $SSL, $url);
        }
    }

    /**
     * Get Updater class version
     *
     * @return string
     *
     */

    public static function get_updater_class_version () {

        return self::$updater_version;
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

        if (!self::$response_error) {

            self::database_connect($database, $username, $password, $hostname);

            if (!self::$response_error) {

                self::$table_prefix=strtolower(self::$table_prefix);
                self::$table_config=strtolower(self::$table_config);

                if (!in_array(self::$table_prefix.self::$table_config, self::get_database_tables())) {

                    self::__initialize_database();
                }

                parent::__construct($codeConn, $secretKey, $SSL, $url);

                self::set_API_version(self::$api_version);

                self::__get_config();
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

    public static function database_connect ($database=null, $username=null, $password=null, $hostname=null) {

        self::__set_database_credentials ($database, $username, $password, $hostname);

        self::$DB = new slyr_SQL(self::$database, self::$username, self::$password, self::$hostname);

        if (self::$DB->error!=null) {

            self::__trigger_error (self::$DB->error, 104);

            return false;
        }

        self::$DB->execute("SET NAMES 'utf8';");

        return true;
    }

    /**
     * Set the prefix for our tables if need change
     *
     * @param string $prefix to the tables
     * @return void
     *
     */

    public static function set_table_prefix ($prefix) {

        self::$table_prefix=$prefix;
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

    private static function __set_database_credentials ($database=null, $username=null, $password=null, $hostname=null) {

        if ($database!=null) { self::$database = $database; }
        if ($database!=null) { self::$username = $username; }
        if ($database!=null) { self::$password = $password; }
        if ($database!=null) { self::$hostname = $hostname; }
    }

    /**
     * Test system requirements
     *
     * @return boolean
     *
     */

    private function __has_system_requirements () {

        if (!extension_loaded('mysql')) {

            self::__trigger_error ('Missing PHP MySQL extension', 103);

            return false;

        } else if (!extension_loaded('CURL')) {

            self::__trigger_error ('Missing CURL extension', 106);

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

        if (!in_array(self::get_response_error(), array(103, 104))) {

            $config_table=self::$table_prefix.self::$table_config;
            $tables      =self::get_database_tables();

            if (!in_array($config_table, $tables)) {

                $SQL="CREATE TABLE IF NOT EXISTS `$config_table` (".
                     "`cnf_id` int(11) NOT NULL AUTO_INCREMENT, ".
                     "`conn_code` varchar(32) NOT NULL, ".
                     "`conn_secret` varchar(32) NOT NULL, ".
                     "`comp_id` int(11) NOT NULL, ".
                     "`last_update` timestamp NOT NULL, ".
                     "`default_language` varchar(6) NOT NULL, ".
                     "`languages` varchar(512) NOT NULL, ".
                     "`conn_schema` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, ".
                     "`data_schema` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, ".
                     "`conn_extra` mediumtext CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL, ".
                     "`updater_version` varchar(10) NOT NULL, ".
                     "PRIMARY KEY (`cnf_id`)".
                     ") ENGINE=MyISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1";

                if (self::$DB->execute(self::$SQL_list[]=$SQL)) {

                    self::$database_tables[]=$config_table;

                    return true;

                } else {

                    self::__trigger_error(self::$DB->error." ($SQL)", 104);
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

    private static function __update_config () {

        if (!in_array(self::get_response_error(), array(103, 104)) && $code=addslashes(self::get_identification_code())) {

            if (self::get_response_action() == 'refresh') {

                $info=self::get_response_table_information(); $schema=array();

                foreach ($info as $table=>$data) {

                    if ($bd_table=strtolower($table)) {

                        $schema[$bd_table]['name']=$table;

                        if (isset($data['table_joins'])) {

                            $schema[$bd_table]['table_joins']=$data['table_joins'];
                        }

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

                unset($info, $data);
            }

            self::get_connectors_list();

            $mode=((!isset(self::$list_connectors['names']) || in_array($code, self::$list_connectors['names']))  ? 'update' : 'insert');

            $SQL="$mode `".                  self::$table_prefix.self::$table_config."` set ".
                 "`conn_code` = '".          $code                                                         ."', ".
                 "`conn_secret` = '".        addslashes(self::get_identification_secret())                 ."', ".
                 "`comp_id` = '".            addslashes(self::get_response_company_ID()                   )."', ".
                 "`last_update` = '".        addslashes(self::get_response_time()                         )."', ".
                 ((self::get_response_action() == 'refresh') ?
                    "`default_language` = '".addslashes(self::get_response_default_language()             )."', ".
                    "`languages` = '".       addslashes(implode(',', self::get_response_languages_used()) )."', ".
                    "`conn_schema` = '".     addslashes(json_encode(self::get_response_connector_schema()))."', ".
                    "`data_schema` = '".     addslashes(json_encode($schema)                              )."', "
                    :
                    ''
                 ).
                 "`updater_version` = '".    addslashes(self::get_response_api_version()                  )."' ".
                 ($mode=='update' ? "where `conn_code`='$code' limit 1" : '');

             if (self::$DB->execute(self::$SQL_list[]=$SQL)) {

                if ($mode=='insert') { self::get_connectors_list($code); }

                self::__get_config('', true);

                 return true;
            }

            self::__trigger_error(self::$DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Set connector credentials
     *
     */

    public static function set_identification ($codeConn, $secretKey=null) {

        if (isset(self::$database_config['conn_code']) && $codeConn!=self::$database_config['conn_code']) {

            self::$database_config=array();
        }

        parent::set_identification($codeConn, $secretKey);
    }

    /**
     * Get configured connector codes
     *
     * @return array
     *
     */

    public static function get_connectors_list ($code=null) {

        if (!isset(self::$list_connectors['names']) || !count(self::$list_connectors['names'])) {

            self::$list_connectors['names']=array();

            $list=self::$DB->execute(self::$SQL_list[]='select `conn_code` from `'.self::$table_prefix.self::$table_config.'`');

            if (count($list)) {

                foreach ($list as $v) { self::$list_connectors['names'][]=$v['conn_code']; }
            }
        }

        if ($code && (!count(self::$list_connectors['names']) || !in_array($code, self::$list_connectors['names']))) {

            self::$list_connectors['names'][]=$code; self::get_connectors_info($code);
        }

        return self::$list_connectors['names'];
    }

    /**
     * Get configured connector data
     *
     * @param string $code for get only data from specified connector
     * @return array
     *
     */

    public static function get_connectors_info ($code=null, $refresh_info = false) {

        if ($refresh_info){ unset(self::$list_connectors['data']); }

        if (!isset(self::$list_connectors['data']) || !count(self::$list_connectors['data']) || ($code && !isset(self::$list_connectors['data'][$code]))) {

            $list=self::$DB->execute(self::$SQL_list[]='select * from `'.self::$table_prefix.self::$table_config.'`'.
                                                        (isset(self::$list_connectors['data'][$code]) ? ' where `conn_code`=\''.addslashes($code).'\' limit 1' : ''));

            if (count($list)) {

                if (!$code or !isset(self::$list_connectors['data'])) self::$list_connectors['data']=array();

                foreach ($list as &$v) {

                    foreach ($v as &$w) { if (substr($w, 0, 1)=='{') { $w=json_decode($w, 1); }} unset($w);

                    self::$list_connectors['data'][$v['conn_code']]=$v;
                }

                unset($v, $w);
            }
        }

        return ($code ? (isset(self::$list_connectors['data'][$code]) ? self::$list_connectors['data'][$code] : array())
                        :
                        self::$list_connectors['data']);
    }

    /**
     * Get extra info from connector
     *
     * @param string $code connector
     * @return array
     *
     */

    public static function get_connector_extra_info ($code) {

        $SQL='select `conn_extra` from `'.self::$table_prefix.self::$table_config.'` where `conn_code`=\''.addslashes($code).'\' limit 1';

        if ($res=self::$DB->execute(self::$SQL_list[]=$SQL)) {

            return json_decode($res[0]['conn_extra'], 1);
        }

        self::__trigger_error(self::$DB->error." ($SQL)", 104);

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

    public static function set_connector_extra_info ($code, $data, $refresh=false) {

        if (is_array($data)) {

            if (!$refresh) {

                $now=self::get_connector_extra_info($code);

                $data=array_merge((array)$now, $data);
            }

            $SQL='update `'.self::$table_prefix.self::$table_config.'` set `conn_extra`=\''.json_encode($data).
                 '\' where `conn_code`=\''.addslashes($code).'\' limit 1';

            if (self::$DB->execute(self::$SQL_list[]=$SQL)) { return true; }

            self::__trigger_error(self::$DB->error." ($SQL)", 104);
        }

        return false;
    }

    /**
     * Get database configurations
     *
     * @return array
     *
     */

    private static function __get_config ($code='', $refresh=false) {

        if (!in_array(self::get_response_error(), array(103, 104)) && self::get_connectors_list()) {

            if (!$code) { $code=addslashes(self::get_identification_code()); }

            if ( $code) {

                if ($refresh || !count(self::$database_config) || self::$database_config['conn_code']!=$code) {

                    $data=self::$DB->execute(self::$SQL_list[]='select * from `'.self::$table_prefix.self::$table_config."` where `conn_code`='$code' limit 1");

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

                } else { return self::$database_config; }
            }
        }
        
        $config = isset($config) ? $config : null;
        
        return self::$database_config = $config;
    }

    /**
     * Construct the real field name
     *
     * @return string
     */

    private static function __get_real_field ($field, $table, $language=null) {

        $table =strtolower($table);
        $schema=self::get_database_table_schema($table, false);
        $fields=self::get_database_table_fields($table);

        if (is_array($schema) && (isset($schema[$field]) || isset($fields[$field]))) {

            $field=$field.((isset($schema[$field]) && $schema[$field]['has_multilingual']) ? '_'.self::__test_language($language) : '');

            if (isset($fields[$field])) { return $field; }
        }

        return '';
    }

    /**
     * Test if language code exist in database
     *
     * @return string
     */

    private static function __test_language ($language) {

        $languages=self::get_languages();

        if (!is_string($language)) {

            $language =self::get_default_language();

            if (!in_array($language, $languages)) { $language=reset($languages); }

        } else {

            if (!in_array($language, $languages)) { $language=($default=self::get_default_language() ? $default : reset($languages)); }
        }

        return $language;
    }

    /**
     * Get table internal name
     *
     * @param $table string database table
     * @return name
     *
     */

    public static function get_database_table_name ($table) {

        if (!self::$database_config) self::__get_config();

        $table=strtolower($table);

        return (isset(self::$database_config['data_schema'][$table]['name']) ? self::$database_config['data_schema'][$table]['name'] : $table);
    }

    /**
     * Get table schema
     *
     * @param $table string database table
     * @param $extended boolean extends multilingual fields
     * @return array
     *
     */

    public static function get_database_table_schema ($table, $extended=true) {

        if (!self::$database_config) self::__get_config();

        $table=strtolower($table);

        if (isset(self::$database_config['data_schema'][$table]['fields'])) {

            $join_fields=array();

            if (    isset(self::$database_config['data_schema'][$table]['table_joins'])) {

                 foreach (self::$database_config['data_schema'][$table]['table_joins'] as $field_id=>$table_rel) {

                     $join_fields[strtolower($field_id)]=array(

                        'type' =>'key',
                        'table'=>$table_rel,
                        'name' =>$field_id
                    );
                }
            }

            if ($extended!=true) {

                $fields=array_merge($join_fields, self::$database_config['data_schema'][$table]['fields']);

            } else {

                $fields=array(); $languages=self::get_languages();

                foreach (self::$database_config['data_schema'][$table]['fields'] as $field=>$info) {

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

    public static function get_database_table_joins ($table) {

        $table=strtolower($table);

        return (isset(self::$database_config['data_schema'][$table]['table_joins'])) ? self::$database_config['data_schema'][$table]['table_joins'] : false;
    }

    /**
     * Update database
     *
     * @param $params array parameters
     * @return string
     *
     */

    public static function update ($params=null, $connector_type=null, $force_refresh=false) {

        if ($code=self::get_identification_code()) {

            if (!self::$database_config) self::__get_config($code);

            if ($force_refresh==true ||
                (isset(self::$database_config['conn_code']) && self::get_identification_code()!=self::$database_config['conn_code'])) {

                self::$database_config['last_update']=null;
            }

            self::get_info(self::$database_config['last_update'], $params, $connector_type);

            if (!self::has_response_error()) {

                self::__update_config();

                if ($force_refresh==true) { self::delete_all(false); }

                self::get_database_tables();

                $tables=array_keys(self::get_response_table_information());

                foreach ($tables as $table) {

                    $table=strtolower($table);

                    if (!in_array(self::$table_prefix.$table, self::$database_tables)) {

                        self::create_database_table($table);

                    } else {

                        self::update_database_table($table);
                    }
                }

                if (count(self::get_response_table_modified_ids()) || count(self::get_response_table_deleted_ids())) {

                    foreach ($tables as $table) {

                        self::update_database_table_data($table);
                    }
                }

                return true;
            }

        } else {

            self::__trigger_error('Invalid connector code', 2);
        }

        return false;
    }

    /**
     * Get database tables
     *
     * @param string $refresh list tables
     * @return array
     */

    public static function get_database_tables ($refresh=false) {

        if (self::$database_tables === null || $refresh == true) {

            $tables=self::$DB->execute(self::$SQL_list[]='SHOW TABLES');

            self::$database_tables=array();

            if (is_array($tables) && count($tables)) {

                foreach ($tables as $v) { self::$database_tables[]=((is_array($v)) ? reset($v) : $v); }
            }
        }

        return self::$database_tables;
    }

    /**
     * Get database table fields
     *
     * @param string $table table name
     * @return array
     */

    public static function get_database_table_fields ($table, $refresh=false) {

        $table=strtolower($table);

        if (!isset(self::$database_fields[$table]) || $refresh==true) {

            self::$database_fields[$table]=array();

            $data=self::$DB->execute(self::$SQL_list[]='SHOW COLUMNS FROM `'.self::$table_prefix.$table.'`');

            if (is_array($data) && count($data)) {

                foreach ($data as $v) { self::$database_fields[$table][$v['Field']]=preg_replace('/^([^\s\(]+).*$/', '\\1', $v['Type']); }
            }
        }

        return self::$database_fields[$table];
    }

    /**
     * Get database last update
     *
     * @return string
     */

    public static function get_database_last_update () {

        return (isset(self::$database_config['last_update']) ? self::$database_config['last_update'] : null);
    }

    /**
     * Get database connector codename
     *
     * @return string
     */

    public static function get_database_connector_code () {

        return (isset(self::$database_config['conn_code']) ? self::$database_config['conn_code'] : null);
    }

    /**
     * Test if table exist in the database
     *
     * @param string $table table name
     * @return boolean
     */

    public static function has_database_table ($table) {

        $table=strtolower($table);

        return (isset(self::$database_tables[self::$table_prefix.$table]) ? true : false);
    }

    /**
     * Returs database type from pseudo type schema
     *
     * @param $type string type
     * @return string
     */

    private static function __get_database_type_schema ($type) {

        return (isset(self::$database_field_types[$type]) ? self::$database_field_types[$type] : self::$database_field_types['string']);
    }

    /**
     * Create database table
     *
     * @param $table string table name
     * @return boolean
     */

    public static function create_database_table ($table) {

        $table=strtolower($table); self::get_database_tables(true);

        if (!in_array(self::$table_prefix.$table, self::$database_tables)) {

            $schema=self::get_database_table_schema($table);

            if (count($schema)) {

                $fields="`{$table}_id` int not null auto_increment primary key, `__conn_id__` varchar(512) NOT NULL";

                foreach ($schema as $field=>$info) {

                    $field=strtolower($field);
                    $type =self::__get_database_type_schema($info['type']);

                    $fields.=", `$field` $type ".self::$database_field_types_charset[$type];
                }

                $sly_table=self::$table_prefix.$table;

                self::$DB->execute(self::$SQL_list[]="DROP TABLE IF EXISTS `$sly_table`");

                $SQL="CREATE TABLE `$sly_table` ($fields) ENGINE=MYISAM DEFAULT CHARSET=latin1 AUTO_INCREMENT=1";

                if (self::$DB->execute(self::$SQL_list[]=$SQL)) {

                    self::$database_tables[]=$sly_table;

                    unset(self::$database_fields[$table]);

                    return true;

                } else {

                    self::__trigger_error(self::$DB->error." ($SQL)", 104);
                }
            }

        } else {

            return self::update_database_table($table);
        }

        return false;
    }

    /**
     * Update database table
     *
     * @param $table string table name
     * @return boolean
     */

    public static function update_database_table ($table) {

        $table=strtolower($table); self::get_database_tables(true);

        if (in_array(self::$table_prefix.$table, self::$database_tables)) {

            if (self::get_response_action()=='refresh') {

                $SQL=(count(self::get_connectors_list())==1 ?

                        "TRUNCATE `".   self::$table_prefix.$table."`;"
                        :
                        "DELETE FROM `".self::$table_prefix.$table."` WHERE fin_in_set('".self::$database_config['conn_id']."', `__conn_id__`);");

                self::$DB->execute(self::$SQL_list[]=$SQL);
            }

            $schema=self::get_database_table_schema($table);

            if (is_array($schema) && count($schema)) {

                self::get_database_table_fields($table, true);

                $fields='';

                foreach ($schema as $field=>$info) {

                    if ($info) {

                        $type=self::__get_database_type_schema($info['type']);

                        $mode=((  !isset(self::$database_fields[$table][$field])) ?

                                'ADD' : (self::$database_fields[$table][$field]!=$type ? "CHANGE `$field` " : ''));

                        if ($mode) {

                            $fields.=(($fields) ? ', ' : '')."$mode `$field` $type ".($type=='bigint' ? ' UNSIGNED' : '').
                                     self::$database_field_types_charset[$type];
                        }
                    }
                }

                if (count(self::get_connectors_list())==1) {

                    foreach (array_keys(self::$database_fields[$table]) as $field) {

                        if ($field!=$table.'_id' && $field!='__conn_id__' && !isset($schema[$field])) {

                            $fields.=(($fields) ? ', ' : '')."DROP `$field`";
                        }
                    }
                }

                if ($fields) {

                    $SQL="ALTER TABLE `".self::$table_prefix.$table."` $fields;";

                    if (self::$DB->execute(self::$SQL_list[]=$SQL)) {

                        unset(self::$database_fields[$table]);

                        return true;

                    } else {

                        self::__trigger_error(self::$DB->error." ($SQL)", 104);
                    }
                }
            }

        } else {

            return self::create_database_table($table);
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

    public static function get_database_table_ids ($table, $extend=false) {

        if (!self::$database_tables) self::get_database_tables();

        $table    =strtolower($table);
        $ids      =array();
        $sly_table=self::$table_prefix.$table;

        if (in_array($sly_table, self::$database_tables)) {

            $SQL="select `{$table}_id` as id".($extend ? ', `__conn_id__` as cid' : '')." from `$sly_table`";

            $res=self::$DB->execute(self::$SQL_list[]=$SQL);

            if ($res!==false) {

                if ($res!==true && count($res)) { foreach ($res as $v) { $ids[]=($extend ? array('conn_id'=>$v['cid'], 'id'=>$v['id']) : $v['id']); }}

                return $ids;

            } else {

                self::__trigger_error(self::$DB->error." ($SQL)", 104);
            }
        }

        return $ids;
    }

     /**
     * Get connector type
     *
     * @return string
     */

    public static function get_connector_type () {

        if (!self::$database_config) self::__get_config();

        return self::$database_config['conn_schema']['connector_type'];
    }

    /**
     * Get default language
     *
     * @return string
     */

    public static function get_default_language () {

        if (!self::$database_config) self::__get_config();

        return self::$database_config['default_language'];
    }

    /**
     * Get languages
     *
     * @return array
     */

    public static function get_languages () {

        if (!self::$database_config) self::__get_config();

        $languages=self::$database_config['languages']; $def_language=self::get_default_language();

        if (isset(self::$database_config['conn_schema']['force_output_default_language']) &&
                  self::$database_config['conn_schema']['force_output_default_language']  && !in_array($def_language, $languages)) {

              $languages[]=$def_language;
        }

        return $languages;
    }

    /**
     * Update batabase tables
     *
     * @return boolean
     */

    public static function update_database_table_data ($table) {

        if (!self::$database_tables) self::get_database_tables();

        $table=strtolower($table);

        $sly_table=self::$table_prefix.$table;

        if (in_array($sly_table, self::$database_tables)) {

            $errors    =false;
            $conn_id   =self::$database_config['conn_id'];
            $table_conn=self::get_database_table_name($table);

            $ids=array();

            foreach (self::get_database_table_ids($table, true) as $v) {

                $ids[$v['id']]=array($v['conn_id'], array_flip(explode(',', $v['conn_id'])));
            }

            if (count(self::get_response_table_modified_ids($table_conn))) {

                $schema=self::get_database_table_schema       ($table);
                $data  =self::get_response_table_modified_data($table_conn);

                if (is_array($schema) && is_array($data) && count($data)) {

                    $languages  =self::get_languages();
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

                            if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                                self::__trigger_error(self::$DB->error." ($SQL)", 104);

                                $errors=true; break;
                            }
                        }
                    }
                }
            }

            if ($dids=self::get_response_table_deleted_ids($table)) {

                foreach ($dids as $k=>$id) {

                    if (isset($ids[$id]) && count($ids[$id][1])>1 && isset($ids[$id][1][$conn_id])) {

                        unset($ids[$id][1][$conn_id]);

                        if (count($ids[$id][1])) {

                            $SQL="update `$sly_table` set `__conn_id__`='".addslashes(implode(',', $ids[$id][1]))."' where `{$table}_id`='$id' limit 1;";

                            if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                                self::__trigger_error(self::$DB->error." ($SQL)", 104);

                                $errors=true;
                            }
                        }

                        unset($dids[$k], $ids[$id]);
                    }
                }

                if (count($dids)) {

                    $SQL="delete from `$sly_table` where `{$table}_id` IN ('".implode("','", $dids)."') limit ".count($dids).';';

                    if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                        self::__trigger_error(self::$DB->error." ($SQL)", 104);

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

    public static function extract ($table, $fields=null, $language=null, $conditions=null, $force_default_language=false, $order=null) {

        if (!self::$database_tables) self::get_database_tables();

        $sly_table=self::$table_prefix.$table;

        if (in_array($sly_table, self::$database_tables)) {

            $language     =self::__test_language($language);
            $base_language=self::get_default_language();

            if ($force_default_language && $language==$base_language) { $force_default_language=false; }

            $schema=self::get_database_table_schema($table, false);

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

                                        if (!$db_field=self::__get_real_field($field, $table, $language)) {

                                            $db_field =self::__get_real_field($field, $table, $base_language);
                                        }

                                        if ($db_field) { $fgroup.=(($fgroup) ? ', ' : '')."`$db_field`"; }
                                    }
                                }

                                if ($fgroup) {

                                    $clause='lower('.((count($sfields)>1) ? "concat($fgroup)" : $fgroup).") like '%".
                                            addslashes(strtolower($param['search']))."%'";
                                }

                            } else if (isset($param['value']) && $field=self::__get_real_field($param['field'], $table, $language)) {

                                $clause="`$field`".(($param['condition']) ? $param['condition'] : '=')."'".addslashes($param['value'])."'";

                                if ($force_default_language && isset($schema[$param['field']]['has_multilingual']) &&
                                    $schema[$param['field']]['has_multilingual']) {

                                    $clause="($clause or `".self::__get_real_field($param['field'], $table, $base_language).'`'.
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

                               if (!$db_field=self::__get_real_field($field, $table, $language)) {

                                $db_field =self::__get_real_field($field, $table, $base_language);
                            }

                            if ($db_field) {

                                if (strtoupper($ord)!='ASC') { $ord='DESC'; }

                                $sql_order.=(($sql_order) ? ', ' : '')."`$db_field` $ord";
                            }
                        }
                    }
                }
                if ($field_title and !$sql_order) {

                    if (!$db_field=self::__get_real_field($field_title, $table, $language)) {

                        $db_field =self::__get_real_field($field_title, $table, $base_language);
                    }

                    if ($db_field) { $sql_order="`$db_field` ASC"; }
                }

                if (!$order) { $order='order by `'.$field_title.'` ASC'; }

                $SQL="select `{$table}_id` as ID, `__conn_id__` as CONN_ID$select from `$sly_table`".
                     (($where) ? ' where '.$where : '').($sql_order ? ' order by '.$sql_order : '');

                $res=self::$DB->execute(self::$SQL_list[]=$SQL);

                if ($res===false) {

                    self::__trigger_error(self::$DB->error." ($SQL)", 104);

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

    public static function delete_all ($delete_config=true) {

        self::$database_tables=self::get_database_tables();

        if (count(self::$database_tables)) {

            $tables = (count(self::$database_config['data_schema']) ? array_keys(self::$database_config['data_schema']) : array());

            if (count($tables)) {

                if ($delete_config == true) { $tables[]=self::$table_config; }

                foreach ($tables as $table) {

                    $sly_table = self::$table_prefix.$table;

                    if (in_array($sly_table, self::$database_tables)) {

                        $SQL="DROP TABLE IF EXISTS `$sly_table`";

                        if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                            self::__trigger_error(self::$DB->error." ($SQL)", 104);
                        }

                        if (($res=array_search($sly_table, self::$database_tables)) !== false) {

                            unset(self::$database_tables[$res]);
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

    public static function delete_connector ($code='', $clean_items=false) {

        $del_ids=array();

        if ($code && self::$database_config) {

            $SQL="delete from `".self::$table_prefix.self::$table_config."` where `conn_code`='$code' limit 1;";

            if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                self::__trigger_error(self::$DB->error." ($SQL)", 104);
            }

            if ($clean_items) {

                $tables=(count(self::$database_config['data_schema']) ? array_keys(self::$database_config['data_schema']) : array());

                if (count($tables)) {

                    $conn_id=self::$database_config['conn_id'];

                    foreach ($tables as $table) {

                        $sly_table=self::$table_prefix.$table;

                        $ids=array();

                        foreach (self::get_database_table_ids($table, true) as $v) {

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

                                        if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                                            self::__trigger_error(self::$DB->error." ($SQL)", 104);

                                            $errors=true;
                                        }
                                    }

                                    unset($ids[$k]);
                                }
                            }

                            if (count($ids)) {

                                $SQL="delete from `$sly_table` where `__conn_id__`='$conn_id' limit ".count($ids).';';

                                if (!self::$DB->execute(self::$SQL_list[]=$SQL)) {

                                    self::__trigger_error(self::$DB->error." ($SQL)", 104);
                                }
                            }
                        }
                    }
                }
            }

            self::$database_config=
            self::$list_connectors=array();
        }

        return $del_ids;
    }

    /**
     * Get list of SQL's executed
     *
     * @return array
     */

    public static function get_database_calls () {

        return self::$SQL_list;
    }

    /**
     * Set debbug
     *
     * @param $active boolean
     *
     */

    public static function set_debbug ($active) {

        self::$debbug=($active ? true : false);
    }

    /**
     * Print information debbuged
     *
     */

    public static function print_debbug () {

        if (self::$debbug!==false) {

            $s="\n\n[SLYR_Updater] List of SQL's:\n".print_r(self::$SQL_list, 1)."\r\n";

            if (self::$debbug=='file') { file_put_contents(dirname(__FILE__).DIRECTORY_SEPARATOR.'_log_updater_'.date('Y-m-d_H-i').'.txt', $s, FILE_APPEND); }
            else                       { echo $s; }

            self::$SQL_list=array();

            return $s;
        }

        return '';
    }
}

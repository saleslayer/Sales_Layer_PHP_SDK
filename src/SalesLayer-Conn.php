<?php
/**
 * $Id$
 *
 * Created by Iban Borras.
 *
 * CreativeCommons License Attribution (By):
 * http://creativecommons.org/licenses/by/4.0/
 *
 * SalesLayer Conn class is a library for connection to SalesLayer API
 *
 * @modified 2015-12-17
 * @version 1.20
 *
 */

class SalesLayer_Conn {

    public  static $version_class               = '1.20';

    public  static $url                         = 'api.saleslayer.com';

    public  static $SSL                         = false;
    public  static $SSL_Cert                    = null;
    public  static $SSL_Key                     = null;
    public  static $SSL_CACert                  = null;

    public  static $connect_API_version         = '1.17';

    public  static $response_error              = 0;
    public  static $response_error_message      = '';

    private static $__codeConn                  = null;
    private static $__secretKey                 = null;

    private static $__group_muticategory        = false;

    public  static $data_returned               = null;
    public  static $response_api_version        = null;
    public  static $response_time               = null;
    public  static $response_action             = null;
    public  static $response_tables_info        = null;
    public  static $response_tables_data        = null;

    public  static $response_table_modified_ids = null;
    public  static $response_table_deleted_ids  = null;
    public  static $response_files_list         = null;
    public  static $response_offline_file       = null;
    public  static $response_waiting_files      = null;

    private static $__error_list                = array(

          '1' => 'Validation error',
          '2' => 'Invalid connector code',
          '3' => 'Wrong unique key',
          '4' => 'Invalid codification key',
          '5' => 'Incorrect date of last_update',
          '6' => 'API version nonexistent',
          '7' => 'Wrong output mode',
          '8' => 'Invalid compression type',
          '9' => 'Invalid private key',
         '10' => 'Service temporarily blocked',
         '11' => 'Service temporarily unavailable',
         '12' => 'Incorrect date-code',
         '13' => 'Date code has expired'
    );

    /**
     * Constructor - if you're not using the class statically
     *
     * @param string $codeConn Code Connector Identificator key
     * @param string $secretKey Secret key
     * @param boolean $SSL Enable SSL
     * @param string $url Url to SalesLayer API connection
     * @param boolean $forceuft8 Set PHP system default charset to utf-8
     * @return void
     *
     */

    public function __construct ($codeConn=null, $secretKey=null, $SSL=false, $url=false, $forceuft8=true) {

        if (self::__has_system_requirements()) {

            if ($forceuft8==true) {

                ini_set('default_charset', 'utf-8');
            }

            if ($codeConn) { self::set_identification ($codeConn, $secretKey); }

            self::set_SSL_connection ($SSL);

            self::set_URL_connection ($url);
        }
    }

    /**
     * Test system requirements
     *
     * @return boolean
     *
     */

    private static function __has_system_requirements () {

        if (!extension_loaded('curl')) {

            self::__trigger_error ('Missing PHP curl extension', 100);

            return false;
        }

        return true;
    }

    /**
     * Create URL for API
     *
     * @param timestamp $last_update last updated database
     * @return string
     */

    private static function __get_api_url ($last_update=false) {

        if (self::$__secretKey != null) {

            $time = time();
            $unic = mt_rand();

            $key  = sha1(self::$__codeConn.self::$__secretKey.$time.$unic);

            $get  = "&time=$time&unique=$unic&key=$key";

        } else {

            $get  = '';
        }

        $URL='http'.((self::$SSL) ? 's' : '').'://'.self::$url.'?code='.urlencode(self::$__codeConn).$get;

        if ($last_update                !== null)  $URL .= '&last_update='.(!is_numeric($last_update) ? strtotime($last_update) : $last_update);
        if (self::$connect_API_version  !== null)  $URL .= '&ver='.urlencode(self::$connect_API_version);
        if (self::$__group_muticategory !== false) $URL .= '&group_category_id=1';

        return $URL;
    }

    /**
     * Clean previous error code
     * 
     */

    private static function __clean_error () {

        self::$response_error         = 0;
        self::$response_error_message = '';
    }

    /**
     * Set the Connector identification and secret key
     *
     * @param string $codeConn Connector Code Identificator
     * @param string $secret Secret Key for secure petitions
     * @return void
     *
     */

    public static function set_identification ($codeConn, $secretKey=null) {

        self::$__codeConn  = $codeConn;
        self::$__secretKey = $secretKey;

        self::__clean_error();
    }

    /**
     * Get the Connector identification code
     *
     * @return connector code
     *
     */

    public static function get_identification_code () {

        return self::$__codeConn;
    }

    /**
     * Get the Connector identification secret
     *
     * @return connector secret
     *
     */

    public static function get_identification_secret () {

        return self::$__secretKey;
    }

    /**
     * Set the SSL true/false connection
     *
     * @param boolean $stat indicator
     * @return void
     *
     */

    public static function set_SSL_connection ($stat) {

          self::$SSL = $stat;
    }

    /**
     * Set SSL client credentials
     *
     * @param string $cert SSL client certificate
     * @param string $key SSL client key
     * @param string $CACert SSL CA cert (only required if you are having problems with your system CA cert)
     * @return void
     *
     */

    public static function set_SSL_credentials ($cert = null, $key = null, $CACert = null) {

        self::$SSL_Cert   = $cert;
        self::$SSL_Key    = $key;
        self::$SSL_CACert = $CACert;
    }

    /**
     * Set the URL for the connection petitions
     *
     * @param string $url base
     * @return void
     *
     */

    public static function set_URL_connection ($url) {

        if ($url) self::$url = $url;
    }

    /**
     * Set the API version to connect
     *
     * @param float $version version number of the API to connect
     * @return void
     *
     */

    public static function set_API_version ($version) {

        self::$connect_API_version = $version;
    }

    /**
     * Set group multicategory products
     *
     * @param boolean $group
     * @return void
     *
     */

    public static function set_group_multicategory ($group) {

        self::$__group_muticategory = $group;
    }

    /**
     * Check if Connector identification have been set
     *
     * @return boolean
     *
     */

    public static function hasConnector () {

        return (self::$__codeConn!==null && self::$__secretKey !== null);
    }

    /**
     * CURL Request to retrieve information
     *
     * @param timestamp $last_update last updated database
     * @param array $params extra parameters for the API
     * @param string $connector_type strict specification of connector type
     * @return array info or false (if error)
     *
     */

    public static function get_info ($last_update=null, $params=null, $connector_type=null) {

        if (self::hasConnector()) {

            set_time_limit(0);

            $ch=curl_init(self::__get_api_url($last_update));

            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 1800); // 30 minutes * 60 seconds
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

            if (self::$SSL && self::$SSL_Cert) {

                curl_setopt($ch, CURLOPT_PORT , 443);

                curl_setopt($ch, CURLOPT_SSLCERT, self::$SSL_Cert);
                curl_setopt($ch, CURLOPT_SSLKEY,  self::$SSL_Key);
                curl_setopt($ch, CURLOPT_CAINFO,  self::$SSL_CACert);
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
            }

            if (is_array($params)) {

                curl_setopt($ch, CURLOPT_POST,       true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, $params);

                if (isset($params['compression']) && $params['compression']) { curl_setopt($ch, CURLOPT_ENCODING, 'gzip'); }
            }

            $response = curl_exec($ch);

            if ($response !== false) {

                self::$data_returned = @json_decode(preg_replace('/^\xef\xbb\xbf/', '', $response), 1);

                curl_close($ch);

                if (self::$data_returned !== false && is_array(self::$data_returned)) {

                    unset($response);

                    if ($connector_type && isset(self::$data_returned['schema']['connector_type']) &&
                        $connector_type !=       self::$data_returned['schema']['connector_type']) {

                        self::__trigger_error('Wrong connector type: '.self::$data_returned['schema']['connector_type'], 105);

                    } else {

                        self::__clean_error();

                        return self::__parsing_json_returned();
                    }

                } else {

                    self::__trigger_error('Void response or malformed: '.$response, 101);
                }

            } else {

                self::__trigger_error('Error connection: '.curl_error($ch), 102);

                curl_close($ch);
            }
        }

        return false;
    }

    /**
     * Return received data
     *
     * @return string or null
     *
     */

    public static function get_data_returned () {

        return self::$data_returned;
    }

    /**
     * Parsing received data
     *
     * @return boolean
     *
     */

    private static function __parsing_json_returned () {

        if (self::$data_returned !== null) {

            self::$response_api_version  = self::$data_returned['version'];
            self::$response_time         = self::$data_returned['time'];

            if (isset(self::$data_returned['error']) && self::$data_returned['error']) {

                if (isset(self::$__error_list[self::$data_returned['error']])) {

                    $message_error = self::$__error_list[self::$data_returned['error']];

                } else {

                    $message_error = 'Unknown error';
                }

                self::__trigger_error($message_error, self::$data_returned['error']);

            } else {

                self::$response_action      = self::$data_returned['action'];

                self::$response_tables_info =
                self::$response_files_list  =
                $image_order_sizes          = array();

                if (isset(self::$data_returned['data_schema_info']) && is_array(self::$data_returned['data_schema_info']) &&
                    count(self::$data_returned['data_schema_info'])) {

                    foreach (self::$data_returned['data_schema_info'] as $table=>$info) {

                        foreach ($info as $field=>$props) {

                            self::$response_tables_info[$table]['fields'][$field]=array(

                                'type'             => $props['type'],
                                'has_multilingual' => (isset($props['language_code']) and $props['language_code']) ? 1 : 0
                            );

                            if (isset($props['language_code']) && $props['language_code']) {

                                self::$response_tables_info[$table]['fields'][$field]['language_code']=$props['language_code'];
                                self::$response_tables_info[$table]['fields'][$field]['basename']     =$props['basename'];
                            }

                            if (isset($props['sizes']) && $props['sizes']) {

                                self::$response_tables_info[$table]['fields'][$field]['image_sizes']  =$props['sizes'];
                            }
                        }
                    }
                }

                self::$response_tables_data        =
                self::$response_table_modified_ids =
                self::$response_table_deleted_ids  = array();

                if (is_array(self::$data_returned['data_schema'])) {

                    foreach (self::$data_returned['data_schema'] as $table=>$info) {

                        $parent_id_field = $table.'_parent_id';

                        if (self::$response_action=='refresh') {

                            foreach ($info as $fname) {

                                if (is_string($fname)) {

                                    if ($fname=='ID_PARENT') {

                                        self::$response_tables_info[$table]['fields'][$parent_id_field]=array(

                                            'type'    => 'key',
                                            'related' => $fname
                                        );

                                    } else if (substr($fname, 0, 3)=='ID_') {

                                        $table_join=substr($fname, 3);

                                        self::$response_tables_info[$table]['table_joins'][$table_join.'_id'] = $table_join;
                                    }
                                }
                            }
                        }

                        self::$response_tables_data[$table]=array('modified'=>array(), 'deleted'=>array());
                        self::$response_tables_info[$table]['count_registers'] =

                            (is_array(self::$data_returned['data'][$table])) ? count(self::$data_returned['data'][$table]) : 0;

                        self::$response_tables_info[$table]['count_modified']  = 0;
                        self::$response_tables_info[$table]['count_deleted']   = 0;

                        if (self::$response_tables_info[$table]['count_registers']) {

                            $id_parent = array_search('ID_PARENT', self::$data_returned['data_schema'][$table]);

                            foreach (self::$data_returned['data'][$table] as $reg=>$fields) {

                                if   ($fields[0] == 'D') {

                                    self::$response_table_deleted_ids[$table][]      =
                                    self::$response_tables_data[$table]['deleted'][] = $fields[1];

                                    self::$response_tables_info[$table]['count_deleted']++;

                                } else {

                                    $data=array();

                                    self::$response_table_modified_ids[$table][] = $data['id'] = $fields[1];

                                    if ($id_parent !== false) $data[$parent_id_field] = (isset($fields[$id_parent])) ? $fields[$id_parent] : 0;

                                    foreach (self::$data_returned['data_schema'][$table] as $ord=>$field) {

                                        $fname=(!is_array($field)) ? $field : key($field);

                                        if ($fname!='STATUS' and $fname!='ID' and $fname!='ID_PARENT') {

                                            if (substr($fname, 0, 3)=='ID_') {

                                                $data[substr($fname, 3).'_id'] = $fields[$ord];

                                            } else {

                                                if (isset($fields[$ord]) and is_array($fields[$ord])
                                                    and isset(   self::$data_returned['data_schema'][$table][$ord][$fname])
                                                    and is_array(self::$data_returned['data_schema'][$table][$ord][$fname])) {

                                                    if (isset($fields[$ord][0]) and $fields[$ord][0]!='U') {

                                                        $data['data'][$fname]=array();

                                                        foreach ($fields[$ord] as $fsub) {

                                                            if (is_array($fsub)) {

                                                                foreach ($fsub as $k=>$a) {

                                                                    if ($k>1) {

                                                                        $ext=self::$data_returned['data_schema'][$table][$ord][$fname][intval($k)];

                                                                        if (is_array($ext)) { $ext=$ext['field']; }

                                                                        $data['data'][$fname][$fsub[1]][$ext]      =
                                                                        self::$response_files_list['list_files'][] = $a;
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }

                                                } else {

                                                    $data['data'][$fname] = (isset($fields[$ord])) ? $fields[$ord] : '';
                                                }
                                            }
                                        }
                                    }

                                    self::$response_tables_data[$table]['modified'][]=$data;
                                    self::$response_tables_info[$table]['count_modified']++;
                                }
                            }
                        }
                    }
                }

                if (isset(self::$data_returned['waiting'])) {

                    self::$response_waiting_files=self::$data_returned['waiting'];
                }

                return true;
            }
        }

        return false;
    }

    /**
     * Set info to API
     *
     * @param array $update_items items data to insert/update
     * @param array $delete_items items data to delete
     ** @param boolean $compression gzip compression transfer
     * @return response to API
     *
     */

    public static function set_info ($update_items=array(), $delete_items=array(), $compression=false) {

        $data=array();

        if (self::hasConnector()) {

            if (is_array($update_items) and count($update_items)) {

                $data['input_data']=array();

                foreach ($update_items as $table => &$items) {

                    if (is_array($items) and count($items)) { $data['input_data'][$table]=$items; }
                }
            }

            if (is_array($delete_items) and count($delete_items)) {

                $data['delete_data']=array();

                foreach ($update_items as $table => &$items) {

                    if (is_array($items) and count($items)) { $data['delete_data'][$table]=$items; }
                }
            }

            unset($update_items, $delete_items, $items);

            if (count($data)) {

                $ch=curl_init(self::__get_api_url());

                if (self::$SSL && self::$SSL_Cert) {

                    curl_setopt($ch, CURLOPT_PORT , 443);

                    curl_setopt($ch, CURLOPT_SSLCERT, self::$SSL_Cert);
                    curl_setopt($ch, CURLOPT_SSLKEY,  self::$SSL_Key);
                    curl_setopt($ch, CURLOPT_CAINFO,  self::$SSL_CACert);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
                }

                curl_setopt($ch, CURLOPT_POST,       true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));

                if ($compression) { curl_setopt($ch, CURLOPT_ENCODING, 'gzip'); }

                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

                $response = curl_exec($ch);

                if ($response !== false) {

                    $response = @json_decode(preg_replace('/^\xef\xbb\xbf/', '', $response), 1);

                    if (isset( $response['input_response'])) {

                        return $response['input_response'];

                    } else {

                        self::__trigger_error('Void response or malformed: '.$response, 101);
                    }

                } else {

                    self::__trigger_error('Error connection: '.curl_error($ch), 102);
                }

                curl_close($ch);
            }
        }

        return false;
    }


    /**
     * Set the error code and message
     *
     * @param string $message error text
     * @param integer $errnum error identificator
     * @return void
     *
     */

    public static function __trigger_error ($message, $errnum) {

        if (self::$response_error === 0) {

            self::$response_error         = $errnum;
            self::$response_error_message = $message;
        }

        //trigger_error($message, E_USER_WARNING);
    }

    /**
     * Check if error
     *
     * @return boolean
     *
     */

    public static function has_response_error () {

        if (self::$response_error || self::$response_time === false) return true;

        return false;
    }

    /**
     * Get error number
     *
     * @return integer
     *
     */

    public static function get_response_error () {

        return self::$response_error;
    }

    /**
     * Get error message
     *
     * @return string
     *
     */

    public static function get_response_error_message () {

        return self::$response_error_message;
    }

    /**
     * Returns the updated UNIX time from Sales Layer server
     *
     * @param string $mode mode of output date (datetime|unix). Default: datetime
     * @return integer
     *
     */

    public static function get_response_time ($mode='datetime') {

        return (self::$response_time) ? (($mode=='datetime') ? date('Y-m-d H:i:s', self::$response_time) : self::$response_time) : false;
    }

    /**
     * Get API version
     *
     * @return string
     *
     */

    public static function get_response_api_version () {

        return self::$response_api_version;
    }

    /**
     * Get Conn class version
     *
     * @return string
     *
     */

    public static function get_conn_class_version () {

        return self::$version_class;
    }

    /**
     * Geaction (update = only changes in the database, or refresh = all database information)
     *
     * @returstring
     *
     */

    public static function get_response_action () {

        return self::$response_action;
    }

    /**
     * Get list of tables
     *
     * @return array
     *
     */

    public static function get_response_list_tables () {

        return array_keys(self::$response_tables_info);
    }

    /**
     * Get information about the structure of tables
     *
     * @return array
     *
     */

    public static function get_response_table_information ($table=null) {

        return ($table === null) ? self::$response_tables_info : self::$response_tables_info[$table];
    }

    /**
     * Get parsed data of tables
     *
     * @return array
     *
     */

    public static function get_response_table_data ($table=null) {

        return ($table === null) ? self::$response_tables_data : self::$response_tables_data[$table];
    }

    /**
     * Get ID's of registers deleted
     *
     * @return array
     *
     */

    public static function get_response_table_deleted_ids ($table=null) {

        return ($table === null) ? self::$response_table_deleted_ids
                                   :
                                   ((isset(self::$response_table_deleted_ids[$table])) ? self::$response_table_deleted_ids[$table] : array());
    }

    /**
     * Get ID's of registers modified
     *
     * @return array
     *
     */

    public static function get_response_table_modified_ids ($table=null) {

        return ($table === null) ? self::$response_table_modified_ids
                                   :
                                   ((isset(self::$response_table_modified_ids[$table])) ? self::$response_table_modified_ids[$table] : array());
    }

    /**
     * Get only the modified information
     *
     * @return array
     *
     */

    public static function get_response_table_modified_data ($table=null) {

        if ($table === null) {

            if (isset(self::$response_tables_data)) {

                $result=array();

                foreach (self::$response_tables_data as $table=>$data) {

                    if (isset($response_tables_data[$table]['modified'])) {

                        $result[$table]=self::$response_tables_data[$table]['modified'];
                    }
                }

                return $result;
            }

        } else if (isset(self::$response_tables_data[$table]['modified'])) {

            return self::$response_tables_data[$table]['modified'];
        }

        return array();
    }

    /**
     * Get the list of all files to download
     *
     * @return array
     *
     */

    public static function get_response_list_modified_files () {

        return self::$response_files_list;
    }

    /**
     * Get information about connector schema
     *
     * @return array
     *
     */

    public static function get_response_connector_schema () {

        if (self::$data_returned !== null and isset(self::$data_returned['schema'])) {

            return self::$data_returned['schema'];
        }

        return null;
    }

    /**
     * Get information about connector type
     *
     * @return array
     *
     */

    public static function get_response_connector_type () {

        if (self::$data_returned !== null and isset(self::$data_returned['schema']['connector_type'])) {

            return self::$data_returned['schema']['connector_type'];
        }

        return null;
    }

    /**
     * Get the default language of company database
     *
     * @return string
     *
     */

    public static function get_response_default_language () {

        if (self::$data_returned !== null and isset(self::$data_returned['schema']['default_language'])) {

            return self::$data_returned['schema']['default_language'];
        }

        return null;
    }

    /**
     * Get the languages used by the company database
     *
     * @return array
     *
     */

    public static function get_response_languages_used () {

        if (self::$data_returned !== null and isset(self::$data_returned['schema']['languages'])) {

            return self::$data_returned['schema']['languages'];
        }

        return null;
    }

    /**
     * Get information about Sales Layer company ID
     *
     * @return number
     *
     */

    public static function get_response_company_ID () {

        if (self::$data_returned !== null and isset(self::$data_returned['schema']['company_ID'])) {

            return self::$data_returned['schema']['company_ID'];
        }

        return null;
    }

    /**
     * Get information about Sales Layer company name
     *
     * @return string
     *
     */

    public static function get_response_company_name () {

        if (self::$data_returned !== null and isset(self::$data_returned['schema']['company_name'])) {

            return self::$data_returned['schema']['company_name'];
        }

        return null;
    }

    /**
     * Get the compact file(s) in the offline mode
     *
     * @return array
     *
     */

    public static function get_response_offline_file () {

        if (is_array(self::$data_returned['output']['offline_files']) && count(self::$data_returned['output']['offline_files'])) {

            return self::$data_returned['output']['offline_files'];
        }

        return null;

    }

    /**
     * Get number of images or files waiting in process
     *
     * @return array
     *
     */

    public static function get_response_waiting_files () {

        if (is_array(self::$response_waiting_files) && count(self::$response_waiting_files)) {

            return self::$response_waiting_files;
        }

        return null;
    }

}

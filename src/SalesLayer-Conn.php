<?php
/**
 * $Id$.
 *
 * Created by Iban Borras.
 *
 * CreativeCommons License Attribution (By):
 * http://creativecommons.org/licenses/by/4.0/
 *
 * SalesLayer Conn class is a library for connection to SalesLayer API
 *
 * @modified 2020-06-22
 *
 * @version 1.33
 */
class SalesLayer_Conn 
{

    public $version_class = '1.33';

    public $url = 'api.saleslayer.com';

    public $SSL        = true;
    public $SSL_Cert   = null;
    public $SSL_Key    = null;
    public $SSL_CACert = null;

    public $connect_API_version = '1.17';

    public $connection_timeout     = 1800;  // 30 minutes * 60 seconds
    public $response_error         = 0;
    public $response_error_message = '';
    public $output_pagination      = 5000;

    public $time_unlimit = true;
    public $memory_limit = ''; // <-- examples: 512M or 1024M
    public $user_abort   = false;

    public $data_returned;   
    public $response_api_version;
    public $response_time;
    public $response_action;
    public $response_tables_info;
    public $response_tables_data;

    public $response_table_modified_ids;
    public $response_table_deleted_ids;
    public $response_files_list;
    public $response_offline_file;
    public $response_waiting_files;
    public $response_next_page;
    public $response_page_count;
    public $response_page_length;

    public $response_input_status;
    public $response_input_errors;
    public $response_input_results;

    public  $response_input_tracking          = '';
    public  $response_input_tracking_status   = '';
    public  $response_input_tracking_percent  = 0;
    public  $response_input_tracking_message  = '';
    private $resonpse_last_time_check         = 0; 

    private $__codeConn  = null;
    private $__secretKey = null;
    private $__keyCypher = 'sha256';
    
    protected $__group_multicategory                    = false;
    protected $__get_same_parent_variants_modifications = false;
    protected $__get_parent_modifications               = false;
    protected $__get_parents_category_tree              = false;

    private $__error_list = [
        '1'  => 'Validation error',
        '2'  => 'Invalid connector code',
        '3'  => 'Wrong unique key',
        '4'  => 'Invalid codification key',
        '5'  => 'Incorrect date of last_update',
        '6'  => 'API version nonexistent',
        '7'  => 'Wrong output mode',
        '8'  => 'Invalid compression type',
        '9'  => 'Invalid private key',
        '10' => 'Service temporarily blocked',
        '11' => 'Service temporarily unavailable',
        '12' => 'Incorrect date-code',
        '13' => 'Date code has expired',
        '14' => 'Updating data. Try later'
    ];

    /**
     * Constructor - if you're not using the class statically.
     *
     * @param string $codeConn  Code Connector Identificator key
     * @param string $secretKey Secret key
     * @param bool   $SSL       Enable SSL
     * @param string $url       Url to SalesLayer API connection
     * @param bool   $forceuft8 Set PHP system default charset to utf-8
     */
    public function __construct($codeConn = null, $secretKey = null, $SSL = null, $url = null, $forceuft8 = true)
    {
        if ($this->__has_system_requirements()) {
            if (true == $forceuft8) {
                ini_set('default_charset', 'utf-8');
            }

            if ($codeConn) {
                $this->set_identification($codeConn, $secretKey);
            }

            if ($SSL !== null) $this->set_SSL_connection($SSL);
            if ($url !== null) $this->set_URL_connection($url);
        }
    }

    /**
     * Test system requirements.
     *
     * @return bool
     */
    private function __has_system_requirements()
    {
        if (!extension_loaded('curl')) {
            $this->__trigger_error('Missing PHP curl extension', 100);

            return false;
        }

        return true;
    }

    /**
     * Create URL for API.
     *
     * @param timestamp $last_update last updated database
     *
     * @return string
     */
    private function __get_api_url($last_update = false)
    {
        if (null != $this->__secretKey) {
            $time    = time();
            $unic    = mt_rand();
            $key     = $this->__codeConn . $this->__secretKey . $time . $unic;
            $key     = ('sha256' == $this->__keyCypher ? hash('sha256', $key) : sha1($key));
            $key_var = ('sha256' == $this->__keyCypher ? 'key256' : 'key');

            $get = "&time=$time&unique=$unic&$key_var=$key";
        } else {
            $get = '';
        }

        $URL = 'http' . (($this->SSL) ? 's' : '') . '://' . $this->url . (strpos($this->url, '?') !== false ? '&' : '?').'code=' . urlencode($this->__codeConn) . $get;

        if ($last_update) {
            $URL .= '&last_update=' . (!is_numeric($last_update) ? strtotime($last_update) : $last_update);
        }
        if (null !== $this->connect_API_version) {
            $URL .= '&ver=' . urlencode($this->connect_API_version);
        }
        if (false !== $this->__group_multicategory) {
            $URL .= '&group_category_id=1';
        }
        if (false !== $this->__get_same_parent_variants_modifications) {
            $URL .= '&same_parent_variants=1';
        }
        if (false !== $this->__get_parent_modifications) {
            $URL .= '&first_parent_level=1';
        }
        if (false !== $this->__get_parents_category_tree) {
            $URL .= '&parents_category_tree=1';
        }

        return $URL;
    }

    /**
     * Clean previous error code.
     */
    private function __clean_error()
    {
        $this->response_error         = 0;
        $this->response_error_message = '';
    }

    /**
     * Set the Connector identification and secret key.
     *
     * @param string $codeConn Connector Code Identificator
     * @param string $secret   Secret Key for secure petitions
     */
    public function set_identification($codeConn, $secretKey = null)
    {
        $this->__codeConn  = $codeConn;
        $this->__secretKey = $secretKey;

        $this->__clean_error();
    }

    /**
     * Get the Connector identification code.
     *
     * @return connector code
     */
    public function get_identification_code()
    {
        return $this->__codeConn;
    }

    /**
     * Get the Connector identification secret.
     *
     * @return connector secret
     */
    public function get_identification_secret()
    {
        return $this->__secretKey;
    }

    /**
     * Set the SSL true/false connection.
     *
     * @param bool $stat indicator
     */
    public function set_SSL_connection($stat)
    {
        $this->SSL = $stat;
    }

    /**
     * Set SSL client credentials.
     *
     * @param string $cert   SSL client certificate
     * @param string $key    SSL client key
     * @param string $CACert SSL CA cert (only required if you are having problems with your system CA cert)
     */
    public function set_SSL_credentials($cert = null, $key = null, $CACert = null)
    {
        $this->SSL_Cert   = $cert;
        $this->SSL_Key    = $key;
        $this->SSL_CACert = $CACert;
    }

    /**
     * Set the URL for the connection petitions.
     *
     * @param string $url base
     */
    public function set_URL_connection($url)
    {
        if ($url) {
            $this->url = $url;
        }
    }

    /**
     * Set the API version to connect.
     *
     * @param float $version version number of the API to connect
     */
    public function set_API_version($version)
    {
        $this->connect_API_version = $version;
    }

    /**
     * Set group multicategory products.
     *
     * @param bool $group
     */
    public function set_group_multicategory($enable)
    {
        $this->__group_multicategory = $enable;
    }

    /**
     * Set value for getting same parent variants modifications on single variant modification.
     *
     * @param bool $enable
     */
    public function set_same_parent_variants_modifications($enable)
    {
        $this->__get_same_parent_variants_modifications = $enable;
    }

    /**
     * Set value for getting modifications/deletions of first level parents.
     *
     * @param bool $enable
     */
    public function set_first_level_parent_modifications($enable)
    {
        $this->__get_parent_modifications = $enable;
    }

    /**
     * Set value for getting modifications/deletions of first level parents.
     *
     * @param bool $enable
     */
    public function set_parents_category_tree($enable)
    {
        $this->__get_parents_category_tree = $enable;
    }

    /**
     * Check if Connector identification have been set.
     *
     * @return bool
     */
    public function hasConnector()
    {
        return null !== $this->__codeConn && null !== $this->__secretKey;
    }

    /**
     * Get info from API
     *
     * @param timestamp $last_update         last updated database
     * @param array     $params              extra parameters for the API
     * @param string    $connector_type      strict specification of connector type
     * @param bool      $add_reference_files return file or image reference names
     *
     * @return array info or false (if error)
     */
    public function get_info($last_update = null, $params = null, $connector_type = null, $add_reference_files = false)
    {
        if ($this->hasConnector()) {

            if ($this->time_unlimit) {
                set_time_limit(0);
            }
            if ($this->memory_limit) {
                ini_set('memory_limit', $this->memory_limit);
            }
            if ($this->user_abort) {
                ignore_user_abort(true);
            }
            if (!is_array($params)) {
                $params = [];
            }
            if ($add_reference_files) {
                
                $params['get_file_refereneces'] = 1;
            }
            if ($this->output_pagination and !isset($params['pagination'])) {
                
                $params['pagination'] = $this->output_pagination;
            }

            $stat = $this->call($this->__get_api_url($last_update), $params);

            if ($stat) {
                    
                if (   $connector_type
                    && isset($this->data_returned['schema']['connector_type'])
                    &&       $this->data_returned['schema']['connector_type'] != $connector_type) {

                    $this->__trigger_error('Wrong connector type: '.$this->data_returned['schema']['connector_type'], 105);

                } else {

                    $this->__clean_error();

                    return $this->__parsing_json_returned();
                }
            } 
        }
        
        return false;
    }

    /**
     * Set pagination data
     */
     public function set_pagination($pagination)
     {
         $this->output_pagination = $pagination;
     }

     /**
     * Get pagination 
     */
     public function get_pagination()
     {
         return $this->output_pagination;
     }

    /**
     * Check for data paging
     *
     * @return bool
     */
    public function have_next_page()
    {
        return ($this->response_next_page ? true : false);
    }

    /**
     * Get info from API
     *
     * @return array info or false (if error)
     */
    public function get_next_page_info()
    {
        if ($this->response_next_page) {

            $this->data_returned = null;

            $stat = $this->call($this->response_next_page);

            if ($stat) {
               
                $this->__clean_error();

                return $this->__parsing_json_returned();
            }
        }

        return false;
    }

    /**
     * Get page count
     *
     * @return integer
     */
    public function get_page_count()
    {
        return ($this->response_page_count ? $this->response_page_count : 0);
    }

    /**
     * Get page length
     *
     * @return integer
     */
     public function get_page_length()
     {
         return ($this->response_page_length ? $this->response_page_length : 0);
     }

    /**
     * Set info to API
     *
     * @param array $update_items items data to insert/update
     * @param array $delete_items items data to delete
     * @param bool  $compression  gzip compression transfer
     * @param bool  $force_directly import directly (as of API 1.18)
     * @param array $extra_params add special extra parameters
     *
     * @return response to API
     */
    public function set_info($update_items = [], $delete_items = [], $compression = false, $force_directly = false, $extra_params = [])
    {
        $params = [];

        if ($this->hasConnector()) {
            
            if (is_array($update_items) && !empty($update_items)) {
                
                $params['input_data'] = [];

                foreach ($update_items as $table => &$items) {
                    if (!empty($items)) { $params['input_data'][$table] = $items; }
                }
                unset($items);
            }

            if (is_array($delete_items) && !empty($delete_items)) {
                
                $params['delete_data'] = [];

                foreach ($delete_items as $table => &$items) {
                    if (is_array($items) && !empty($items)) { $params['delete_data'][$table] = $items; }
                }
                unset($items);
            }

            if (is_array($extra_params) && !empty($extra_params)) {
          
                foreach ($extra_params as $key => &$data) {
                    if (!empty($data)) { $params[$key] = $data; }
                }
                unset($data);
            }
            
            unset($update_items, $delete_items, $extra_params);

            if (!empty($params)) {

                if ($force_directly) {
                    $params['input_data_directly'] = 1;
                }


                if ($compression) {
                    $params['compression'] = 1;
                }

                $stat = $this->call($this->__get_api_url(), $params);

                if ($stat && is_array($this->data_returned)) {

                    $stat = $this->__parsing_json_returned();

                    return (floatval($this->connect_API_version) > 1.17 ? $stat : $this->data_returned['input_response']);
                }
            }
        }

        return false;
    }

    /**
     * Check input status
     *
     * @return integer (0 = ok, 1 = warnings, 2 = error)
     */
    public function get_input_status()
    {
        if ($this->response_input_status) {

            return $this->response_input_status;
        }

        return 0;
    }

    /**
     * Check if the input has generated errors
     *
     * @return bool
     */
    public function have_input_errors()
    {
        return ($this->response_input_status == 2 ? true : false);
    }

    /**
     * Check if the input has generated warnings
     *
     * @return bool
     */
    public function have_input_warnings()
    {
        return ($this->response_input_status == 1 ? true : false);
    }

    /**
     * Get input errors
     *
     * @return array width errors
     */
    public function get_input_errors()
    {
        if ($this->response_input_status && is_array($this->response_input_errors) && !empty($this->response_input_errors)) {

            return $this->response_input_errors;
        }

        return [];
    }

    /**
     * Get input benchmarks
     *
     * @return array width these keys: 'items_affected', 'new_items', 'updated_items', 'deleted_items'
     */
    public function get_input_results()
    {
        return (is_array($this->response_input_results) ? $this->response_input_results : []);
    }

    /**
     * Check if have input tracking info
     *
     * @return bool
     */
    public function have_input_tracking()
    {
        return ($this->response_input_tracking ? true : false);
    }
 
    /**
     * Update input tracking from API
     *
     * @return array info
     */

    public function check_input_tracking()
    {
        if ($this->response_input_tracking && time() > $this->resonpse_last_time_check) {

            $stat = $this->call($this->response_input_tracking);

            $this->resonpse_last_time_check = time();

            if ($stat) {
            
                $this->__clean_error();
                $this->__parsing_json_returned();

                if (!$this->response_input_tracking_status) {

                    $this->response_input_tracking_status  = 'end';
                    $this->response_input_tracking_percent = 100;
                    $this->response_input_tracking_message = '';
                }

                return true;
            }

            $this->response_input_tracking_status  = 'error';
            $this->response_input_tracking_percent = 0;
            $this->response_input_tracking_message = '';

        } else if (   !$this->response_input_tracking 
                   &&  $this->response_input_tracking_status != 'error' 
                   && isset($this->data_returned['input_response'])
                   &&       $this->data_returned['input_response']['result'] == 2) {

            $this->response_input_tracking_status  = 'error';
            $this->response_input_tracking_percent = 0;
            $this->response_input_tracking_message = '';
        }

        return false;
    }

    /**
     * Get input tracking status from API
     *
     * @return array info
     */
    
    public function get_input_tracking_status()
    {
        $this->check_input_tracking();

        return $this->response_input_tracking_status;
    }

    /**
     * Get input tracking percentage from API
     *
     * @return array info
     */
    
     public function get_input_tracking_percent()
     {
        $this->check_input_tracking();
 
        return $this->response_input_tracking_percent;
     }

     /**
     * Get input tracking percentage from API
     *
     * @return array info
     */
    
     public function get_input_tracking_message()
     {
        $this->check_input_tracking();
 
        return $this->response_input_tracking_message;
     }

    /**
     * CURL Request to retrieve information.
     *
     * @param string    $url                 API URL for call
     * @param array     $params              extra parameters for the API
     * @param array     $post                POST data
     *
     * @return array info or false (if error)
     */
     public function call ($url, $params = []) {

        if ($url and preg_match('/^https?:\/\/'.preg_quote(preg_replace('/^([^\/?]+[\/?]).*$/', '\\1', $this->url), '/').'/i', $url)) {

            $ch = curl_init($url);

            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, $this->connection_timeout);

            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
            curl_setopt($ch, CURLOPT_FOLLOWLOCATION, true);
            curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, false);
            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);

            if ($this->SSL && $this->SSL_Cert) {
                curl_setopt($ch, CURLOPT_PORT,    443);
                curl_setopt($ch, CURLOPT_SSLCERT, $this->SSL_Cert);
                curl_setopt($ch, CURLOPT_SSLKEY,  $this->SSL_Key);
                curl_setopt($ch, CURLOPT_CAINFO,  $this->SSL_CACert);
            }

            if (is_array($params) and !empty($params)) {

                if (isset($params['compression']) && $params['compression']) {
                    curl_setopt($ch, CURLOPT_ENCODING, 'gzip');
                }

                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, (is_array($params) ? http_build_query($params) : $params)); unset($params);  
            }

            $response = curl_exec($ch);

            if (false !== $response) {

                $this->data_returned = json_decode(preg_replace('/^\xef\xbb\xbf/', '', $response), 1);

                if (false !== $this->data_returned && is_array($this->data_returned)) {

                    return true;

                } else {
                    $this->__trigger_error('Void response or malformed: '.$response, 101);
                }
            } else {
                $this->__trigger_error('Error connection: '.curl_error($ch), 102);
            }
            
            curl_close($ch);

        } else {
            $this->__trigger_error('Incorrect URL call: '.$url, 100);
        }

        return false;
    }

    /**
     * Return received data.
     *
     * @return string or null
     */
     public function get_data_returned()
     {
         return $this->data_returned;
     }
 
     /**
      * Parsing received data.
      *
      * @return bool
      */
     private function __parsing_json_returned()
     {

        if (null !== $this->data_returned) {
 
            if (isset($this->data_returned['version'])) {
            
                $this->response_api_version = $this->data_returned['version'];
            }
            if (isset($this->data_returned['time'])) {
            
                $this->response_time = $this->data_returned['time'];
            }
            if (isset($this->data_returned['action'])) {
                
                $this->response_action = $this->data_returned['action'];
            }

            if (isset($this->data_returned['error']) && $this->data_returned['error']) {

                 if (isset($this->__error_list[$this->data_returned['error']])) {
                     $message_error = $this->__error_list[$this->data_returned['error']];
                 } else {
                     $message_error = 'API error';
                 }
 
                 $this->__trigger_error($message_error, $this->data_returned['error']);

            } else {
                
                $status = false;

                if (isset($this->data_returned['data'])) {

                    if (      isset($this->data_returned['data_schema_info'])
                        && is_array($this->data_returned['data_schema_info'])
                        &&   !empty($this->data_returned['data_schema_info'])) {

                        $this->response_tables_info = [];

                        foreach ($this->data_returned['data_schema_info'] as $table => $info) {
    
                            foreach ($info as $field => $props) {
    
                                $this->response_tables_info[$table]['fields'][$field] = [
                                    'type'             => (('ID' == $field or substr($field, 0, 3) == 'ID_') ? ($props['type'] == 'list' ? 'multi-key' : 'key') : $props['type']),
                                    'sanitized'        => (isset($props['sanitized']) ? $props['sanitized'] : (isset($props['basename']) ? $props['basename'] : $field)),
                                    'has_multilingual' => ((isset($props['language_code']) and $props['language_code']) ? 1 : 0),
                                ];
    
                                if (isset($props['language_code']) && $props['language_code']) {
                                    $this->response_tables_info[$table]['fields'][$field]['language_code'] = $props['language_code'];
                                    $this->response_tables_info[$table]['fields'][$field]['basename']      = $props['basename'];
                                }
                                if (isset($props['title']) && $props['title']) {
                                    $this->response_tables_info[$table]['fields'][$field]['title'] = $props['title'];
                                } elseif (isset($props['titles']) && $props['titles']) {
                                    $this->response_tables_info[$table]['fields'][$field]['titles'] = $props['titles'];
                                } else {
                                    $this->response_tables_info[$table]['fields'][$field]['title'] = $field;
                                }
                                if (isset($props['tag_translations']) && $props['tag_translations']) {
                                    $this->response_tables_info[$table]['fields'][$field]['tag_translations'] = $props['tag_translations'];
                                } 
                                if (isset($props['table_key'])) {
                                    $this->response_tables_info[$table]['fields'][$field]['title'] = $props['table_key'];
                                }
                                if (isset($props['sizes']) && $props['sizes']) {
                                    $this->response_tables_info[$table]['fields'][$field]['image_sizes'] = $props['sizes'];
                                }
                                if (isset($props['origin']) && $props['origin']) {
                                    $this->response_tables_info[$table]['fields'][$field]['origin'] = $props['origin'];
                                }
                            }
                        }
                    }

                    if (isset($this->data_returned['data_schema'])) {
    
                        $this->response_tables_schema = $this->data_returned['data_schema'];
        
                        foreach ($this->data_returned['data_schema'] as $table => $info) {
    
                            foreach ($info as $ord => $fname) {
    
                                if (is_string($fname)) {
    
                                    if (substr($fname, 0, 3) == 'ID_' and 'ID_PARENT' != $fname) {
    
                                        $this->response_tables_info[$table]['table_joins'][$fname] = preg_replace('/^ID_/', '', $fname);
                                    }
                                }
                            }

                            $this->response_tables_info[$table]['count_registers'] = (isset($this->data_returned['data'][$table]) ? count($this->data_returned['data'][$table]) : 0);
                            $this->response_tables_info[$table]['count_modified']  =
                            $this->response_tables_info[$table]['count_deleted']   = 0;
                        }
                    }

                    $this->response_tables_data        =
                    $this->response_table_modified_ids =
                    $this->response_table_deleted_ids  =
                    $this->response_files_list         = [];

                    if ($this->data_returned['data'] && !empty($this->response_tables_info)) {

                        $tables = array_keys($this->response_tables_info);

                        foreach ($tables as $table) {

                            $this->response_tables_data[$table]       = ['modified' => [], 'count_modified' => 0, 'deleted' => [], 'count_deleted' => 0];
                            $this->response_table_deleted_ids[$table] = [];


                            if (isset($this->data_returned['data'][$table]) && is_array($this->data_returned['data'])) {

                                foreach ($this->data_returned['data'][$table] as &$fields) {

                                    if (!empty($fields)) {

                                        if ('D' == $fields[0]) {

                                            $this->response_table_deleted_ids[$table][]      =
                                            $this->response_tables_data[$table]['deleted'][] = $fields[1];
                                            $this->response_tables_data[$table]['count_deleted'] ++;

                                        } else {

                                            $data                                        = [ 'data' => [] ];
                                            $this->response_table_modified_ids[$table][] = $data['ID'] = $fields[1];

                                            foreach ($this->response_tables_schema[$table] as $ord => $field) {

                                                $fname = (!is_array($field)) ? $field : key($field);

                                                if (!in_array($fname, ['STATUS', 'ID'])) {

                                                    if ('REF' == $fname or substr($fname, 0, 3) == 'ID_') {

                                                        $data[$fname] = (isset($fields[$ord]) ? $fields[$ord] : null);

                                                    } else if (    isset($fields[$ord])
                                                            and is_array($fields[$ord])
                                                            and    isset($this->response_tables_schema[$table][$ord][$fname])
                                                            and is_array($this->response_tables_schema[$table][$ord][$fname])) {

                                                        $data['data'][$fname] = [];

                                                        if (isset($fields[$ord][0]) and 'U' != $fields[$ord][0]) {

                                                            foreach ($fields[$ord] as $fsub) {

                                                                if (is_array($fsub)) {
                                                                    foreach ($fsub as $k => $a) {
                                                                        if ($k > 1) {
                                                                            $ext = $this->response_tables_schema[$table][$ord][$fname][intval($k)];
                                                                            if (is_array($ext)) { $ext = $ext['field']; }
                                                                            $data['data'][$fname][$fsub[1]][$ext] =
                                                                            $this->response_files_list[]          = $a;
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }

                                                    } else {
                                                        $data['data'][$fname] = (isset($fields[$ord]) ? $fields[$ord] : '');
                                                    }
                                                }
                                            }

                                            $this->response_tables_data[$table]['modified'][] = $data;
                                            $this->response_tables_data[$table]['count_modified'] ++;
                                        }
                                    }
                                }
                                unset($fields);
                            }
                        }
                    }
    
                    if (isset($this->data_returned['waiting'])) {
                        $this->response_waiting_files = $this->data_returned['waiting'];
                    }
    
                    if (isset($this->data_returned['next_page'])) {

                        $this->response_next_page   = $this->data_returned['next_page'];
                        $this->response_page_count  = $this->data_returned['page_count'];
                        $this->response_page_length = $this->data_returned['page_length'];
                        
                    } else {

                        $this->response_next_page   = '';
                        $this->response_page_count  =
                        $this->response_page_length = 0;
                    }

                    $status =  true;
                }

                if (isset($this->data_returned['input_response'])) {

                    if (   isset($this->data_returned['input_response']['status_tracking'])
                        &&       $this->data_returned['input_response']['status_tracking']) {

                        $this->response_input_tracking = $this->data_returned['input_response']['status_tracking'];
                    }

                    if (isset($this->data_returned['input_response'])) {

                        $this->response_input_status  = $this->data_returned['input_response']['result'];
                        $this->response_input_errors  = (isset($this->data_returned['input_response']['error']) ? $this->data_returned['input_response']['error']  : '');
                        $this->response_input_results = [];

                        foreach (['items_affected', 'new_items', 'updated_items', 'deleted_items'] as $key) {

                            $this->response_input_results[$key] = (isset($this->data_returned['input_response'][$key]) ? $this->data_returned['input_response'][$key] : 0);
                        }
                    }

                    $status = true;
                } 
                
                if (isset($this->data_returned['input_percent_completion'])) {

                    $this->response_input_tracking_status  = $this->data_returned['input_status'];
                    $this->response_input_tracking_percent = $this->data_returned['input_percent_completion'];
                    $this->response_input_tracking_message = $this->data_returned['input_action_message'];

                    $status = true;
                }

                if (empty($this->response_input_tracking) and empty($this->response_input_tracking_status)) {

                    $this->response_input_tracking_status  = 'end';
                    $this->response_input_tracking_percent = 100;
                    $this->response_input_tracking_message = '';
                }

                return $status;
            }
        }
 
        return false;
     }

    /**
     * Set the error code and message.
     *
     * @param string $message error text
     * @param int    $errnum  error identificator
     */
    public function __trigger_error($message, $errnum)
    {
        if (0 === $this->response_error) {
            $this->response_error         = $errnum;
            $this->response_error_message = $message;
        }

        //trigger_error($message, E_USER_WARNING);
    }

    /**
     * Check if error.
     *
     * @return bool
     */
    public function has_response_error()
    {
        if ($this->response_error || false === $this->response_time) {
            return true;
        }

        return false;
    }

    /**
     * Get error number.
     *
     * @return int
     */
    public function get_response_error()
    {
        return $this->response_error;
    }

    /**
     * Get error message.
     *
     * @return string
     */
    public function get_response_error_message()
    {
        return $this->response_error_message;
    }

    /**
     * Returns the updated UNIX time from Sales Layer server.
     *
     * @param string $mode mode of output date (datetime|unix). Default: datetime
     *
     * @return int
     */
    public function get_response_time($mode = 'datetime')
    {
        return $this->response_time ? ('datetime' === $mode ? date('Y-m-d H:i:s', $this->response_time) : $this->response_time) : false;
    }

    /**
     * Get API version.
     *
     * @return string
     */
    public function get_response_api_version()
    {
        return $this->response_api_version;
    }

    /**
     * Get Conn class version.
     *
     * @return string
     */
    public function get_conn_class_version()
    {
        return $this->version_class;
    }

    /**
     * Geaction (update = only changes in the database, or refresh = all database information).
     *
     * @returstring
     */
    public function get_response_action()
    {
        return $this->response_action;
    }

    /**
     * Get list of tables.
     *
     * @return array
     */
    public function get_response_list_tables()
    {
        return array_keys($this->response_tables_info);
    }

    /**
     * Get information about the structure of tables.
     *
     * @return array
     */
    public function get_response_table_information($table = null)
    {
        return (null === $table) ? $this->response_tables_info : $this->response_tables_info[$table];
    }

    /**
     * Get parsed data of tables.
     *
     * @return array
     */
    public function get_response_table_data($table = null)
    {
        return (null === $table) ? $this->response_tables_data : $this->response_tables_data[$table];
    }

    /**
     * Get ID's of registers deleted.
     *
     * @return array
     */
    public function get_response_table_deleted_ids($table = null)
    {
        return (null === $table) ? $this->response_table_deleted_ids
							       :
							       ((isset($this->response_table_deleted_ids[$table])) ? $this->response_table_deleted_ids[$table] : []);
    }

    /**
     * Get ID's of registers modified.
     *
     * @return array
     */
	public function get_response_table_modified_ids($table = null)
    {
        return (null === $table ? $this->response_table_modified_ids
	           :
	           (isset($this->response_table_modified_ids[$table]) ? $this->response_table_modified_ids[$table] : []));
    }

    /**
     * Get only the modified information.
     *
     * @return array
     */
    public function get_response_table_modified_data($table = null)
    {
        if (null === $table) {
            if (isset($this->response_tables_data)) {
                $result = array();

                foreach (array_keys($this->response_tables_data) as $table) {
                    if (isset($this->response_tables_data[$table]['modified'])) {
                        $result[$table] = $this->response_tables_data[$table]['modified'];
                    }
                }

                return $result;
            }
        } elseif (isset($this->response_tables_data[$table]['modified'])) {
            return $this->response_tables_data[$table]['modified'];
        }

        return array();
    }

    /**
     * Get the list of all files to download.
     *
     * @return array
     */
    public function get_response_list_modified_files()
    {
        return $this->response_files_list;
    }

    /**
     * Get information about connector schema.
     *
     * @return array
     */
    public function get_response_connector_schema()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema'])) {
            return $this->data_returned['schema'];
        }

        return null;
    }

    /**
     * Get language titles of tables
     *
     * @return array
     */
    public function get_response_sanitized_table_names()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']) and isset($this->data_returned['schema']['sanitized_table_names'])) {

            return $this->data_returned['schema']['sanitized_table_names'];
        }

        return null;
    }
    /**
     * Get language titles of tables
     *
     * @return array
     */
    public function get_response_table_titles()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']) and isset($this->data_returned['schema']['language_table_names'])) {

            return $this->data_returned['schema']['language_table_names'];
        }

        return null;
    }

    /**
     * Get table joins
     *
     * @return array
     *
     */
    public function get_response_table_joins($table = null)
    {
        if (null !== $this->response_tables_info and is_array($this->response_tables_info)) {
            if (null === $table) {
                $list = [];
                foreach ($this->response_tables_info as $table => $info) { 
                    $list[$table] = (isset($info['table_joins']) ? $info['table_joins'] : []); 
                }

                return $list;
            }

            return (isset($this->response_tables_info[$table]['table_joins']) ? $this->response_tables_info[$table]['table_joins'] : []);
        }

        return null;
    }

    /**
     * Get information about connector type.
     *
     * @return array
     */
    public function get_response_connector_type()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']['connector_type'])) {
            return $this->data_returned['schema']['connector_type'];
        }

        return null;
    }

    /**
     * Get the default language of company database.
     *
     * @return string
     */
    public function get_response_default_language()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']['default_language'])) {
            return $this->data_returned['schema']['default_language'];
        }

        return null;
    }

    /**
     * Get the languages used by the company database.
     *
     * @return array
     */
    public function get_response_languages_used()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']['languages'])) {
            return $this->data_returned['schema']['languages'];
        }

        return null;
    }

    /**
     * Get information about Sales Layer company ID.
     *
     * @return number
     */
    public function get_response_company_ID()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']['company_ID'])) {
            return $this->data_returned['schema']['company_ID'];
        }

        return null;
    }

    /**
     * Get information about Sales Layer company name.
     *
     * @return string
     */
    public function get_response_company_name()
    {
        if (null !== $this->data_returned and isset($this->data_returned['schema']['company_name'])) {
            return $this->data_returned['schema']['company_name'];
        }

        return null;
    }

    /**
     * Get the compact file(s) in the offline mode.
     *
     * @return array
     */
    public function get_response_offline_file()
    {
        if (is_array($this->data_returned['output']['offline_files']) && !empty($this->data_returned['output']['offline_files'])) {
            return $this->data_returned['output']['offline_files'];
        }

        return null;
    }

    /**
     * Get the detailed information of the scheme.
     *
     * @return array
     */
     public function get_schema_information()
     {
         if (isset($this->data_returned['data_schema_info'])) {
             return $this->data_returned['data_schema_info'];
         }
 
         return null;
     }

    /**
     * Get number of images or files waiting in process.
     *
     * @return array
     */
    public function get_response_waiting_files()
    {
        if (is_array($this->response_waiting_files) && !empty($this->response_waiting_files)) {
            return $this->response_waiting_files;
        }

        return null;
    }

    /**
     * Get field titles.
     *
     * @return array
     */
    public function get_response_field_titles($table = null)
    {
        $titles = [];

        if (null !== $this->data_returned) {
            if (!$table) {
                $tables = array_keys($this->response_tables_info);
            } else {
                $tables = [$table];
            }

            $languages = $this->data_returned['schema']['languages'];

            foreach ($tables as $table) {
                $titles[$table] = [];

                if (isset($this->response_tables_info[$table])
                    and is_array($this->response_tables_info[$table]['fields'])
                    and !empty($this->response_tables_info[$table]['fields'])) {
                    foreach ($this->response_tables_info[$table]['fields'] as $field => &$info) {
                        if (!in_array($field, ['ID', 'ID_PARENT'])) {
                            $field_name = (isset($info['basename']) ? $info['basename'] : $field);

                            if (isset($info['titles']) and !empty($info['titles'])) {
                                $titles[$table][$field_name] = $info['titles'];
                            } else {
                                if (!isset($titles[$table][$field_name])) {
                                    $titles[$table][$field_name] = [];
                                }

                                $title = ((isset($info['title']) and $info['title']) ? $info['title'] : $field_name);

                                if (isset($info['language_code'])) {
                                    $titles[$table][$field_name][$info['language_code']] = $title;
                                } else {
                                    foreach ($languages as $lang) {
                                        $titles[$table][$field_name][$lang] = $title;
                                    }
                                }
                            }
                        }
                    }

                    unset($info);
                }
            }
        }

        return $titles;
    }

    /**
     * Get field titles in certain language.
     *
     * @param string $language (ISO 639-1)
     *
     * @return array
     */
    public function get_response_language_field_titles($language, $table = null)
    {
        $titles = [];

        if (null !== $this->data_returned) {
            if (!$table) {
                $tables = array_keys($this->response_tables_info);
            } else {
                $tables = [$table];
            }

            $default_language = $this->data_returned['schema']['default_language'];

            foreach ($tables as $table) {
                $titles[$table] = [];

                if (isset($this->response_tables_info[$table])
                    and is_array($this->response_tables_info[$table]['fields'])
                    and !empty($this->response_tables_info[$table]['fields'])) {
                    foreach ($this->response_tables_info[$table]['fields'] as $field => &$info) {
                        if (!in_array($field, ['ID', 'ID_PARENT'])) {
                            if (isset($info['language_code'])) {
                                if ($info['language_code'] == $language) {
                                    if (isset($info['titles']) and !empty($info['titles'])) {
                                        if (isset($info['titles'][$language])) {
                                            $titles[$table][$field] = $info['titles'][$language];
                                        } elseif (isset($info['titles'][$default_language])) {
                                            $titles[$table][$field] = $info['titles'][$default_language];
                                        } else {
                                            $titles[$table][$field] = reset($info['titles']);
                                        }
                                    } elseif (isset($info['title'])) {
                                        $titles[$table][$field] = $info['title'];
                                    } else {
                                        $titles[$table][$field] = $info['basename'];
                                    }
                                }
                            } else {
                                $titles[$table][$field] = $field;
                            }
                        }
                    }
                    unset($info);
                }
            }
        }

        return $titles;
    }
}

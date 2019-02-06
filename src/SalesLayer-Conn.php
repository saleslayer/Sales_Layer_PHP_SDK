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
 * @modified 2018-10-19
 *
 * @version 1.25
 */
class SalesLayer_Conn
{
    public $version_class = '1.25';

    public $url = 'api.saleslayer.com';

    public $SSL = false;
    public $SSL_Cert = null;
    public $SSL_Key = null;
    public $SSL_CACert = null;

    public $connect_API_version = '1.17';

    public $response_error = 0;
    public $response_error_message = '';

    private $__codeConn = null;
    private $__secretKey = null;
    private $__keyCypher = 'sha256'; // <-- or 'sha1'

    protected $__group_multicategory = false;

    public $data_returned = null;
    public $response_api_version = null;
    public $response_time = null;
    public $response_action = null;
    public $response_tables_info = null;
    public $response_tables_data = null;

    public $response_table_modified_ids = null;
    public $response_table_deleted_ids = null;
    public $response_files_list = null;
    public $response_offline_file = null;
    public $response_waiting_files = null;

    public $time_unlimit = true;
    public $memory_limit = '';      // <-- examples: 512M or 1024M
    public $user_abort = false;

    private $__error_list = array(
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
         '13' => 'Date code has expired',
         '14' => 'Updating data. Try later',
    );

    /**
     * Constructor - if you're not using the class statically.
     *
     * @param string $codeConn  Code Connector Identificator key
     * @param string $secretKey Secret key
     * @param bool   $SSL       Enable SSL
     * @param string $url       Url to SalesLayer API connection
     * @param bool   $forceuft8 Set PHP system default charset to utf-8
     */
    public function __construct($codeConn = null, $secretKey = null, $SSL = false, $url = false, $forceuft8 = true)
    {
        if ($this->__has_system_requirements()) {
            if ($forceuft8 == true) {
                ini_set('default_charset', 'utf-8');
            }

            if ($codeConn) {
                $this->set_identification($codeConn, $secretKey);
            }

            $this->set_SSL_connection($SSL);
            $this->set_URL_connection($url);
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
        if ($this->__secretKey != null) {
            $time = time();
            $unic = mt_rand();
            $key = $this->__codeConn.$this->__secretKey.$time.$unic;
            $key = ($this->__keyCypher == 'sha256' ? hash('sha256', $key) : sha1($key));
            $key_var = ($this->__keyCypher == 'sha256' ? 'key256' : 'key');

            $get = "&time=$time&unique=$unic&$key_var=$key";
        } else {
            $get = '';
        }

        $URL = 'http'.(($this->SSL) ? 's' : '').'://'.$this->url.'?code='.urlencode($this->__codeConn).$get;

        if ($last_update) {
            $URL .= '&last_update='.(!is_numeric($last_update) ? strtotime($last_update) : $last_update);
        }
        if ($this->connect_API_version !== null) {
            $URL .= '&ver='.urlencode($this->connect_API_version);
        }
        if ($this->__group_multicategory !== false) {
            $URL .= '&group_category_id=1';
        }

        return $URL;
    }

    /**
     * Clean previous error code.
     */
    private function __clean_error()
    {
        $this->response_error = 0;
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
        $this->__codeConn = $codeConn;
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
        $this->SSL_Cert = $cert;
        $this->SSL_Key = $key;
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
    public function set_group_multicategory($group)
    {
        $this->__group_multicategory = $group;
    }

    /**
     * Check if Connector identification have been set.
     *
     * @return bool
     */
    public function hasConnector()
    {
        return $this->__codeConn !== null && $this->__secretKey !== null;
    }

    /**
     * CURL Request to retrieve information.
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

            $ch = curl_init($this->__get_api_url($last_update));

            curl_setopt($ch, CURLOPT_CONNECTTIMEOUT, 1800); // 30 minutes * 60 seconds
            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

            if ($this->SSL && $this->SSL_Cert) {
                curl_setopt($ch, CURLOPT_PORT, 443);
                curl_setopt($ch, CURLOPT_SSLCERT, $this->SSL_Cert);
                curl_setopt($ch, CURLOPT_SSLKEY, $this->SSL_Key);
                curl_setopt($ch, CURLOPT_CAINFO, $this->SSL_CACert);
                curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
            }

            if ($add_reference_files) {
                if (!is_array($params)) {
                    $params = [];
                }

                $params['get_file_refereneces'] = 1;
            }

            if (is_array($params)) {
                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($params));

                if (isset($params['compression']) && $params['compression']) {
                    curl_setopt($ch, CURLOPT_ENCODING, 'gzip');
                }
            }

            $response = curl_exec($ch);

            if ($response !== false) {
                $this->data_returned = json_decode(preg_replace('/^\xef\xbb\xbf/', '', $response), 1);

                curl_close($ch);

                if ($this->data_returned !== false && is_array($this->data_returned)) {
                    
                    unset($response);

                    if ($connector_type
                        && isset($this->data_returned['schema']['connector_type'])
                        && $connector_type != $this->data_returned['schema']['connector_type']) {
                        
                        $this->__trigger_error('Wrong connector type: '.$this->data_returned['schema']['connector_type'], 105);

                    } else {
        
                        $this->__clean_error();

                        return $this->__parsing_json_returned();
                    }

                } else {
                    $this->__trigger_error('Void response or malformed: '.$response, 101);
                }

            } else {

                $this->__trigger_error('Error connection: '.curl_error($ch), 102);
                
                curl_close($ch);
            }
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
        if ($this->data_returned !== null) {
            $this->response_api_version = $this->data_returned['version'];
            $this->response_time = $this->data_returned['time'];

            if (isset($this->data_returned['error']) && $this->data_returned['error']) {
                if (isset($this->__error_list[$this->data_returned['error']])) {
                    $message_error = $this->__error_list[$this->data_returned['error']];
                } else {
                    $message_error = 'API error';
                }

                $this->__trigger_error($message_error, $this->data_returned['error']);
            } else {
                $this->response_action = $this->data_returned['action'];
                $this->response_tables_info =
                $this->response_files_list = [];

                if (isset($this->data_returned['data_schema_info'])
                    && is_array($this->data_returned['data_schema_info'])
                    && count($this->data_returned['data_schema_info'])) {
                    foreach ($this->data_returned['data_schema_info'] as $table => $info) {
                        foreach ($info as $field => $props) {
                            $this->response_tables_info[$table]['fields'][$field] = [
                                'type' => $props['type'],
                                'has_multilingual' => ((isset($props['language_code']) and $props['language_code']) ? 1 : 0),
                            ];

                            if (isset($props['language_code']) && $props['language_code']) {
                                $this->response_tables_info[$table]['fields'][$field]['language_code'] = $props['language_code'];
                                $this->response_tables_info[$table]['fields'][$field]['basename'] = $props['basename'];
                            }

                            if (isset($props['title']) && $props['title']) {
                                $this->response_tables_info[$table]['fields'][$field]['title'] = $props['title'];
                            } elseif (isset($props['titles']) && $props['titles']) {
                                $this->response_tables_info[$table]['fields'][$field]['titles'] = $props['titles'];
                            }

                            if (isset($props['sizes']) && $props['sizes']) {
                                $this->response_tables_info[$table]['fields'][$field]['image_sizes'] = $props['sizes'];
                            }
                        }
                    }
                }

                $this->response_tables_data =
                $this->response_table_modified_ids =
                $this->response_table_deleted_ids = [];

                if (is_array($this->data_returned['data_schema'])) {
                    foreach ($this->data_returned['data_schema'] as $table => $info) {
                        $parent_id_field = $table.'_parent_id';

                        if ($this->response_action == 'refresh') {
                            foreach ($info as $fname) {
                                if (is_string($fname)) {
                                    if ($fname == 'ID_PARENT') {
                                        $this->response_tables_info[$table]['fields'][$parent_id_field] = array(
                                            'type' => 'key',
                                            'related' => $fname,
                                        );
                                    } elseif (substr($fname, 0, 3) == 'ID_') {
                                        $table_join = substr($fname, 3);

                                        $this->response_tables_info[$table]['table_joins'][$table_join.'_id'] = $table_join;
                                    }
                                }
                            }
                        }

                        $this->response_tables_data[$table] = ['modified' => [], 'deleted' => []];
                        $this->response_tables_info[$table]['count_registers'] =

                            (is_array($this->data_returned['data'][$table])) ? count($this->data_returned['data'][$table]) : 0;

                        $this->response_tables_info[$table]['count_modified'] = 0;
                        $this->response_tables_info[$table]['count_deleted'] = 0;

                        if ($this->response_tables_info[$table]['count_registers']) {
                            $id_parent = array_search('ID_PARENT', $this->data_returned['data_schema'][$table]);

                            foreach ($this->data_returned['data'][$table] as $fields) {
                                if ($fields[0] == 'D') {
                                    $this->response_table_deleted_ids[$table][] =
                                    $this->response_tables_data[$table]['deleted'][] = $fields[1];

                                    ++$this->response_tables_info[$table]['count_deleted'];
                                } else {
                                    $data = array();

                                    $this->response_table_modified_ids[$table][] = $data['id'] = $fields[1];

                                    if ($id_parent !== false) {
                                        $data[$parent_id_field] = (isset($fields[$id_parent])) ? $fields[$id_parent] : 0;
                                    }

                                    foreach ($this->data_returned['data_schema'][$table] as $ord => $field) {
                                        $fname = (!is_array($field)) ? $field : key($field);

                                        if ($fname != 'STATUS' and $fname != 'ID' and $fname != 'ID_PARENT') {
                                            if (substr($fname, 0, 3) == 'ID_') {
                                                $data[substr($fname, 3).'_id'] = $fields[$ord];
                                            } else {
                                                if (isset($fields[$ord]) and is_array($fields[$ord])
                                                    and isset($this->data_returned['data_schema'][$table][$ord][$fname])
                                                    and is_array($this->data_returned['data_schema'][$table][$ord][$fname])) {
                                                    $data['data'][$fname] = [];

                                                    if (isset($fields[$ord][0]) and $fields[$ord][0] != 'U') {
                                                        foreach ($fields[$ord] as $fsub) {
                                                            if (is_array($fsub)) {
                                                                foreach ($fsub as $k => $a) {
                                                                    if ($k > 1) {
                                                                        $ext = $this->data_returned['data_schema'][$table][$ord][$fname][intval($k)];

                                                                        if (is_array($ext)) {
                                                                            $ext = $ext['field'];
                                                                        }

                                                                        $data['data'][$fname][$fsub[1]][$ext] =
                                                                        $this->response_files_list['list_files'][] = $a;
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

                                    $this->response_tables_data[$table]['modified'][] = $data;
                                    ++$this->response_tables_info[$table]['count_modified'];
                                }
                            }
                        }
                    }
                }

                if (isset($this->data_returned['waiting'])) {
                    $this->response_waiting_files = $this->data_returned['waiting'];
                }

                return true;
            }
        }

        return false;
    }

    /**
     * Set info to API.
     *
     * @param array $update_items items data to insert/update
     * @param array $delete_items items data to delete
     * @param bool  $compression  gzip compression transfer
     *
     * @return response to API
     */
    public function set_info($update_items = array(), $delete_items = array(), $compression = false)
    {
        $data = array();

        if ($this->hasConnector()) {
            if (is_array($update_items) and count($update_items)) {
                $data['input_data'] = array();

                foreach ($update_items as $table => &$items) {
                    if (is_array($items) and count($items)) {
                        $data['input_data'][$table] = $items;
                    }
                }
            }

            if (is_array($delete_items) and count($delete_items)) {
                $data['delete_data'] = array();

                foreach ($update_items as $table => &$items) {
                    if (is_array($items) and count($items)) {
                        $data['delete_data'][$table] = $items;
                    }
                }
            }

            unset($update_items, $delete_items, $items);

            if (count($data)) {
                $ch = curl_init($this->__get_api_url());

                if ($this->SSL && $this->SSL_Cert) {
                    curl_setopt($ch, CURLOPT_PORT, 443);

                    curl_setopt($ch, CURLOPT_SSLCERT, $this->SSL_Cert);
                    curl_setopt($ch, CURLOPT_SSLKEY, $this->SSL_Key);
                    curl_setopt($ch, CURLOPT_CAINFO, $this->SSL_CACert);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
                }

                curl_setopt($ch, CURLOPT_POST, true);
                curl_setopt($ch, CURLOPT_POSTFIELDS, json_encode($data));

                if ($compression) {
                    curl_setopt($ch, CURLOPT_ENCODING, 'gzip');
                }

                curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);

                $response = curl_exec($ch);

                if ($response !== false) {
                    $response = @json_decode(preg_replace('/^\xef\xbb\xbf/', '', $response), 1);

                    if (isset($response['input_response'])) {
                        return $response['input_response'];
                    }
                    if (isset($response['error'])) {
                        $this->__trigger_error('API error', $response['error']);
                    } else {
                        $this->__trigger_error('Void response or malformed: '.$response, 101);
                    }
                } else {
                    $this->__trigger_error('Error connection: '.curl_error($ch), 102);
                }

                curl_close($ch);
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
        if ($this->response_error === 0) {
            $this->response_error = $errnum;
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
        if ($this->response_error || $this->response_time === false) {
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
        return $this->response_time ? ($mode === 'datetime' ? date('Y-m-d H:i:s', $this->response_time) : $this->response_time) : false;
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
        return ($table === null) ? $this->response_tables_info : $this->response_tables_info[$table];
    }

    /**
     * Get parsed data of tables.
     *
     * @return array
     */
    public function get_response_table_data($table = null)
    {
        return ($table === null) ? $this->response_tables_data : $this->response_tables_data[$table];
    }

    /**
     * Get ID's of registers deleted.
     *
     * @return array
     */
    public function get_response_table_deleted_ids($table = null)
    {
        return ($table === null) ? $this->response_table_deleted_ids
                                   :
                                   ((isset($this->response_table_deleted_ids[$table])) ? $this->response_table_deleted_ids[$table] : array());
    }

    /**
     * Get ID's of registers modified.
     *
     * @return array
     */
    public function get_response_table_modified_ids($table = null)
    {
        return ($table === null) ? $this->response_table_modified_ids
                                   :
                                   ((isset($this->response_table_modified_ids[$table])) ? $this->response_table_modified_ids[$table] : array());
    }

    /**
     * Get only the modified information.
     *
     * @return array
     */
    public function get_response_table_modified_data($table = null)
    {
        if ($table === null) {
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
        if ($this->data_returned !== null and isset($this->data_returned['schema'])) {
            return $this->data_returned['schema'];
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
        if ($this->data_returned !== null and isset($this->data_returned['schema']['connector_type'])) {
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
        if ($this->data_returned !== null and isset($this->data_returned['schema']['default_language'])) {
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
        if ($this->data_returned !== null and isset($this->data_returned['schema']['languages'])) {
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
        if ($this->data_returned !== null and isset($this->data_returned['schema']['company_ID'])) {
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
        if ($this->data_returned !== null and isset($this->data_returned['schema']['company_name'])) {
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
        if (is_array($this->data_returned['output']['offline_files']) && count($this->data_returned['output']['offline_files'])) {
            return $this->data_returned['output']['offline_files'];
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
        if (is_array($this->response_waiting_files) && count($this->response_waiting_files)) {
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

        if ($this->data_returned !== null) {
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
                    and count($this->response_tables_info[$table]['fields'])) {
                    foreach ($this->response_tables_info[$table]['fields'] as $field => &$info) {
                        if (!in_array($field, ['ID', 'ID_PARENT'])) {
                            $field_name = (isset($info['basename']) ? $info['basename'] : $field);

                            if (isset($info['titles']) and count($info['titles'])) {
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

        if ($this->data_returned !== null) {
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
                    and count($this->response_tables_info[$table]['fields'])) {
                    foreach ($this->response_tables_info[$table]['fields'] as $field => &$info) {
                        if (!in_array($field, ['ID', 'ID_PARENT'])) {
                            if (isset($info['language_code'])) {
                                if ($info['language_code'] == $language) {
                                    if (isset($info['titles']) and count($info['titles'])) {
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

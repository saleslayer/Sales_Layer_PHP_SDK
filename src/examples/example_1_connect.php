<?php

	/**
	 * $Id$
	 *
	 * SalesLayer conn class usage
	 */

	if (!class_exists('SalesLayer_Conn')) require dirname(__FILE__).DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.'SalesLayer-Conn.php';


    $connector_id = '__Sales_Layer_connector_code__';
	$secret_key   = '__Sales_Layer_secret__';

	// Instantiate the class
	$SLconn = new SalesLayer_Conn($connector_id, $secret_key);

	$SLconn->set_API_version('1.16');

	$SLconn->get_info();

	if ($SLconn->has_response_error())

		echo "<h4>Error:</h4>\n\n Code: ".$SLconn->get_response_error().
			 "<br>\nMessage: ".           $SLconn->get_response_error_message();

	else {

		echo "<h4>Response OK</h4>\n".
			 "<p>".
			 "API version: <b>".            $SLconn->get_response_api_version()            ."</b><br />\n".
			 "Action: <b>".                 $SLconn->get_response_action()                 ."</b><br />\n".
			 "Time: <b>".                   $SLconn->get_response_time()                   ."</b> (GMT 0)<br />\n".
             "Default language: <b>".       $SLconn->get_response_default_language()       ."</b><br /><br />\n".

			 "Information:<br />".  print_r($SLconn->get_response_table_information(), 1)  ."<br /><br />\n".

			 "Modified ID's:<br />".print_r($SLconn->get_response_table_modified_ids(), 1) ."<br /><br />\n".

			 "Deleted ID's:<br />". print_r($SLconn->get_response_table_deleted_ids(), 1)  ."<br /><br />\n".

			 "Data:<br />".         print_r($SLconn->get_response_table_data(), 1)         ."<br /><br />\n".

             "List files:<br />".   print_r($SLconn->get_response_list_modified_files(), 1)."<br /><br />\n".

			 "</p>\n";

	}

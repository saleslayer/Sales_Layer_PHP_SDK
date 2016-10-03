Sales Layer SDK for PHP
=======================

This SDK contains all the logic and connection methods to Sales Layer’s customizable API.

Samples
=======

	if (!class_exists('SalesLayer_Conn')) require_once 'SalesLayer-Conn.php';

    $connector_id = 'CN000H0000C000';
	$secret_key   = 'f035324ba8f98a6f33c05ee1ce36ef29';

	// Instantiate the class
	$SLconn = new SalesLayer_Conn($connector_id, $secret_key);

	$SLconn->set_API_version('1.17');

	$SLconn->get_info();

	if ($SLconn->has_response_error()) {

		echo "<h4>Error:</h4>\n\n Code: ".$SLconn->get_response_error().
			 "<br>\nMessage: ".           $SLconn->get_response_error_message();

	} else {

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
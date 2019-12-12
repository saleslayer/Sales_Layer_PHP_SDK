<?php

	/**
	 * $Id$
	 *
	 * SalesLayer updater class usage
	 */

	if (!class_exists('SalesLayer_Updater')) require dirname(__FILE__).DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.'SalesLayer-Updater.php';

    $dbname       = '__test';
	$dbhost       = 'localhost';
	$dbusername   = 'root';
	$dbpassword   = '';

	$CONNS = [

		'__Sales_Layer_connector_code__'=>'__Sales_Layer_secret__',
  		//'__other_Sales_Layer_connector_code__'=>'__other_Sales_Layer_secret__'
	];

	// Instantiate the class
	$SLupdate = new SalesLayer_Updater($dbname, $dbusername, $dbpassword, $dbhost);

	echo '<h4>Updater class version: '.$SLupdate->get_updater_class_version().'</h4>';

    if ($SLupdate->has_response_error()) {

		echo "<h4>Error:</h4>\n\n Code: ".$SLupdate->get_response_error().
			 "<br>\nMessage: ".           $SLupdate->get_response_error_message();

	} else {

		foreach ($CONNS as $codeConn => $secretKey) {

	        $SLupdate->set_identification($codeConn, $secretKey);

            echo "<h3>Connector: $codeConn</h3>\n";

			// Updater!
	        $SLupdate->update();

			if ($SLupdate->has_response_error()) {

				echo "<h4>Error:</h4>\n\n Code: ".$SLupdate->get_response_error().
					 "<br>\nMessage: ".           $SLupdate->get_response_error_message();

			} else {

	            echo "<h4>Response OK</h4>\n<p>";

				$fields = ['section_name', 'section_description', 'image_reference'];

	            echo "Query 1 (lang: en):<br>\n".print_r($SLupdate->extract('catalogue', $fields, 'en'), 1)."<br /><br />\n";

                $fields     = ['product_name'];
	            $conditions = [

					'1' => [
						'field' =>'product_name',
						'search'=>'acme'
					]
				];

	            echo "Query 2 (lang: fr, width conditions):<br>\n".print_r($SLupdate->extract('products', $fields, 'fr', $conditions), 1)."<br /><br />\n";
				echo "</p>\n";
			}
		}

		// Printem el debug

        $SLupdate->print_debbug();
	}

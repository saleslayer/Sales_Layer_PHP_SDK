<h1 align="center">
  <a href="https://support.saleslayer.com">
    <img src="https://saleslayer.com/assets/images/logo.svg" alt="Sales Layer for Developers" width="230"></a>
  </a>
  <br><br>
  Sales Layer's PHP SDK
  <br>
</h1>

<h4 align="center">This is the official PHP SDK for Sales Layer's PIM Platform.</h4>

## Description

This SDK contains all the logic and connection methods to Sales Layer’s customizable API. And you can find more detailed documentation at our [website](https://support.saleslayer.com/category/api-sdk-examples "Title").

## Requirements

PHP 5.6 and later

## Started
### With Github
Access [github](https://github.com/saleslayer/Sales_Layer_PHP_SDK "Sales_Layer_PHP_SDK") and download the package.

### With Composer
Using [Composer](https://getcomposer.org/ "Composer") is the recommended way to install the Sales Layer SDK for PHP. The SDK is available via [Packagist](https://packagist.org/ "packagist.org") under the **saleslayer/saleslayer-php-sdk** package. If Composer is installed globally on your system, you can run the following in the base directory of your project to add the SDK as a dependency:

```php
composer require saleslayer/saleslayer-php-sdk
```

## Examples

```php
<?php

if (!class_exists('SalesLayer_Conn')) require_once 'SalesLayer-Conn.php';

$connector_id = 'CN000H0000C000';
$secret_key   = 'f035324ba8f98a6f33c05ee1ce36ef29';

// Instantiate the class
$SLconn = new SalesLayer_Conn($connector_id, $secret_key);

$SLconn->set_API_version('1.17');

$SLconn->get_info();

if ($SLconn->has_response_error()) {

	echo "<h4>Error:</h4>\n\n Code: ".$SLconn->get_response_error(). "<br>\nMessage: " . $SLconn->get_response_error_message();

} else {

	echo "<h4>Response OK</h4>\n".
		 "<p>".
		 "API version: <b>". $SLconn->get_response_api_version() ."</b><br />\n".
		 "Action: <b>". $SLconn->get_response_action() ."</b><br />\n".
		 "Time: <b>". $SLconn->get_response_time() ."</b> (GMT 0)<br />\n".
		 "Default language: <b>". $SLconn->get_response_default_language() ."</b><br /><br />\n".
		 "Information:<br />". print_r($SLconn->get_response_table_information(), 1) ."<br /><br />\n".
		 "Modified ID's:<br />" .print_r($SLconn->get_response_table_modified_ids(), 1) ."<br /><br />\n".
		 "Deleted ID's:<br />". print_r($SLconn->get_response_table_deleted_ids(), 1)  ."<br /><br />\n".
		 "Data:<br />". print_r($SLconn->get_response_table_data(), 1) ."<br /><br />\n".
		 "List files:<br />". print_r($SLconn->get_response_list_modified_files(), 1)."<br /><br />\n".
		 "</p>\n";
		 
	}
	
?>
```

## Documentation & Important notes

> :warning: **A Sales Layer account might be needed to access the documentation**

##### PHP SDK Examples https://support.saleslayer.com/api-sdk-examples/example-1-simple-export

##### API Documentation https://support.saleslayer.com/api/introduction


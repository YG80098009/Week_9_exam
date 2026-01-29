from typing import List, Dict, Any
from db import get_db_connection

def get_customers_by_credit_limit_range(cursor):
    """Return customers with credit limits outside the normal range."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT customerName, creditLimit 
    FROM customers 
    WHERE creditLimit < 10000 OR creditLimit > 100000;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    print(result)
    cursor.close()
    connection.close()
    return {"result": result}

def get_orders_with_null_comments(cursor):
    """Return orders that have null comments."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT orderNumber, comments FROM orders 
    WHERE comments IS NULL 
    ORDER BY orderDate;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}

def get_first_5_customers(cursor):
    """Return the first 5 customers."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT customerName, contactLastName, contactFirstName FROM customers 
    ORDER BY contactLastName DESC LIMIT 5;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}

def get_payments_total_and_average(cursor):
    """Return total and average payment amounts."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT 
    SUM(amount) as total_payments, 
    AVG(amount) as average_payment, 
    MIN(amount) as min_payment, 
    MAX(amount) as max_payment 
    FROM payments;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}

def get_employees_with_office_phone(cursor):
    """Return employees with their office phone numbers."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT e.firstName, e.lastName, o.phone FROM employees e 
    JOIN offices o ON e.officeCode = o.officeCode;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}

def get_customers_with_shipping_dates(cursor):
    """Return customers with their order shipping dates."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT c.customerName, o.shippedDate FROM customers c
    LEFT JOIN orders o ON c.customerNumber = o.customerNumber;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}

def get_customer_quantity_per_order(cursor):
    """Return customer name and quantity for each order."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT c.customerName, od.quantityOrdered FROM customers c
    JOIN orders o ON c.customerNumber = o.customerNumber
    JOIN orderdetails od ON o.orderNumber = od.orderNumber
    ORDER BY c.customerName;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}


def get_customers_payments_by_lastname_pattern(cursor, pattern: str = "son"):
    """Return customers and payments for last names matching pattern."""
    connection = get_db_connection()
    cursor = connection.cursor(dictionary=True)
    query = """
    SELECT c.customerName, e.firstName, e.lastName, 
	SUM(p.amount) AS total_paid FROM customers c
    JOIN employees e ON c.salesRepEmployeeNumber = e.employeeNumber
    JOIN payments p ON c.customerNumber = p.customerNumber
    WHERE c.contactFirstName LIKE '%Mu%' OR c.contactFirstName LIKE '%ly%'
    GROUP BY c.customerName, e.firstName, e.lastName
    ORDER BY total_paid DESC;
    """
    cursor.execute(query)
    result = cursor.fetchall()
    cursor.close()
    connection.close()
    return {"result": result}



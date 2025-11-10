# Prompt Guidance for Text-to-SQL Chatbot 🤖

The goal of this guidance is to help you phrase your questions in a way the system can understand perfectly every time. Think of it as teaching the system your language!

## 📏 **1. Focus on What and How to Count/Measure**

Instead of using general verbs like "return" or "give me," specify the **type of calculation** you want. This clarifies the SQL aggregation function (like **COUNT**, **SUM**, **AVG**).

| Expected User Prompt                    | SQL-Friendly Phrasing (Clear) | Key Concept                                  |
| --------------------                    | ----------------------------- | -----------                                  |
| How **many** cars are there?            | **Count** of cars             | Use **Count** to know the number of records. |
| What is the **total** sales amount?     | **Sum** of sales amount       | Use **Sum** to get a total value.            |
| What is the **average** price?          | **Average** price             | Use **Average** to find the mean value.      |
| What is the **highest** speed recorded? | **Maximum** speed             | Use **Maximum** to find the largest value.   |
| What is the **lowest** temperature?     | **Minimum** temperature       | Use **Minimum** to find the smallest value.  |

💡 Pro Tip: Always specify the column you are counting or summing.

✔️ Good: Count of **cars**

💯 Better: Count of **cars** grouped by **model year**

## 🔠 **2. Grouping and Categorization (The "By" Rule)**

When you want a breakdown of a calculation, use the phrase **"grouped by"** or simply **"by."** This tells the system how to categorize the results.

| Expected User Prompt                    | SQL-Friendly Phrasing (Clear)                 | Key Concept                         |
| --------------------                    | -----------------------------                 | -----------                         |
| Return amount of cars **by** model year | **Count** of cars **grouped by** model year   | Creates a result row for each model year. |
| Show me the sales for **each** region   | **Sum** of sales **by** region                | Aggregates sales data for every region. |
| Total inventory **per** warehouse       | **Sum** of inventory **grouped by** warehouse | Breaks down the sum by warehouse.   |

## 🥇 **3. Ranking and Limiting Results (The "Top N" Rule)**

To find the best, worst, or a specific number of records, use "Top N" or "Bottom N" and explicitly state the order. This corresponds to **ORDER BY** and **LIMIT**.

| Expected User Prompt                   | SQL-Friendly Phrasing (Clear)                               | Key Concept  |
| --------------------                   | -----------------------------                               | -----------  |
| Give me **top 3** brands by car volume | **Top 3 Count** of cars by brand, **ordered descending**    | **Top** implies a high value. Use **descending** order. |
| Show the **least** performing products | **Bottom 5 Sum** of sales by product, **ordered ascending** | **Bottom/Least** implies a low value. Use **ascending** order. |
| List the 10 **most** expensive cars    | **Top 10** price of cars, **ordered descending**            | Clearly state the value you are ordering by. |

⬆️⬇️ **Key Ordering Words:**

* **Descending (DESC):** For **Highest, Top, Most, Largest.**
* **Ascending (ASC):** For **Lowest, Bottom, Least, Smallest.**

## **4. Filtering (The "Where" Rule)**

To apply a condition (to filter the data), use the word "where" followed by the specific condition.

| Expected User Prompt       | SQL-Friendly Phrasing (Clear)                       | Key Concept                                 |
| --------------------       | -----------------------------                       | -----------                                 |
| How many blue cars?        | **Count** of cars **where** color is **'Blue'**     | Filters the data to **only** include blue cars. |
| Sales for 2024             | **Sum** of sales **where** year **is** 2024                     | Filters for a specific year.                |
| List customers in NY or CA | **List** customers **where** state **is** 'NY' **or** state **is** 'CA' | Use **'or'** for multiple acceptable values.    |
| Transactions over $1000    | **Count** of transactions **where** amount **is greater than** 1000 | Use **is greater than**, **is less than**, **is equal to**. |

💡 **Pro Tip:** When filtering text (like names, colors, or types), put the value in **single quotes** (e.g., **'Blue'**).

## 📋 **Summary Template for Success**

For best results, try to structure your question like this:

*1* CALCULATION/LIST + *2* GROUPING (Optional) + *3* FILTERING (Optional) + *4* RANKING (Optional)

| Part           | Example Phrase                                                                         |
| ----           | ---------------                                                                        |
| *Calculation*: | **Count of cars**                                                                      |
| *Grouping*:    | **grouped by Model Year**                                                              |
| *Filtering*:   | **where Model Year is 2023**                                                           |
| *Ranking*:     | **ordered descending**                                                                 |
| *Full Prompt*: | **Count of cars, grouped by Model Year, where Model Year is 2023, ordered descending** |
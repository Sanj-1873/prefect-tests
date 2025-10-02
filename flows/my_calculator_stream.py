import time
import subprocess
import pyautogui # You'll need to install this: pip install pyautogui
from prefect import flow, task

# --- macOS-Specific RPA Functions ---

def open_calculator():
    """Opens the macOS Calculator using AppleScript."""
    print("Opening macOS Calculator...")
    # Use 'osascript' to run AppleScript to launch the app
    subprocess.run(['osascript', '-e', 'tell application "Calculator" to activate'], check=True)
    time.sleep(2) # Give it time to launch

def close_calculator():
    """Quits the macOS Calculator using AppleScript."""
    print("Closing macOS Calculator...")
    subprocess.run(['osascript', '-e', 'tell application "Calculator" to quit'], check=True)

def hands_press_key(key: str):
    """Uses pyautogui to press a key."""
    # For calculator clear, this is usually the 'c' key
    pyautogui.press(key)
    time.sleep(0.1)

def perform_calculation_rpa(expression: str) -> str:
    """
    Simulates calculation input using pyautogui.
    NOTE: Reading the result is the hardest part on any OS and often requires
    advanced techniques like OCR (Optical Character Recognition) or clipboard reading.
    For this example, we'll only simulate the input.
    """
    
    # 1. Clear the calculator
    hands_press_key('c')
    
    # 2. Convert expression (e.g., "15+25") to key presses
    # This is a basic conversion, real logic would be complex
    key_mapping = {'+': '+', '-': '-', '*': '*', '/': '/'}
    
    for char in expression:
        if char.isdigit():
            pyautogui.press(char)
        elif char in key_mapping:
            pyautogui.press(key_mapping[char])
        time.sleep(0.1)
        
    # 3. Press the equals key
    pyautogui.press('=')
    
    time.sleep(1) 
    
    # *** IMPORTANT ***: You would need complex logic here to read the screen 
    # and return the actual calculation result as a string.
    # A common technique is to copy the result to the clipboard:
    pyautogui.hotkey('command', 'c') # Command + C to copy the result
    time.sleep(0.5)
    
    try:
        # Get the result from the clipboard (requires 'pyperclip' to be installed)
        import pyperclip
        result = pyperclip.paste()
        # Clean up the result (remove commas, spaces, etc.)
        return result.strip().replace(',', '')
    except ImportError:
        print("Install 'pyperclip' to read the clipboard result.")
        return "Result_Read_Error"
    
# The rest of your Prefect flow and task logic (using the @flow and @task decorators) 
# remains the same, as it calls these underlying functions.
# --- Prefect Task Definition ---

@task(name="perform_single_calculation", retries=3, retry_delay_seconds=5)
def perform_calculation(expression: str) -> dict:
    """
    Executes a single calculation using RPA.
    
    Returns:
        A dict with the result or an error message.
    """
    logger = get_run_logger()
    logger.info(f"Attempting calculation: {expression}")
    
    try:
        # 1. Use your RPA tool to get the result
        result = perform_calculation_rpa(expression)
        
        # 2. Clear the calculator for the next calculation
        hands_press_key("c")
        time.sleep(0.5)
        
        logger.info(f"Successfully calculated {expression} = {result}")
        return {"expression": expression, "status": "success", "result": result}
        
    except Exception as e:
        logger.error(f"Failed calculation {expression}: {e}")
        # Note: If the task fails, Prefect handles the retry.
        # If it ultimately fails, it will bubble up as a Task failure.
        raise # Re-raise the exception to mark the task as failed
    

# --- Prefect Flow Definition ---

# Your deployment refers to 'calculator_automation_stream'
@flow(name="calculator_automation_stream")
async def calculator_automation_stream(
    calculations: list[str],
):
    """
    Calculator automation that streams calculations as independent tasks.
    """
    
    open_calculator() # This runs synchronously before the tasks start
    
    results = []
    
    try:
        # Map/Stream the calculations to be run as independent task runs
        # This will run all calculations concurrently if possible,
        # or in sequence based on available workers/resources.
        result_futures = perform_calculation.map(calculations)
        
        # Wait for all tasks to complete and collect their results
        for future in result_futures:
            try:
                # .result() will raise an exception if the task failed
                results.append(future.result())
            except Exception as e:
                # Handle cases where a specific task run failed
                calc_expression = future.kwargs.get('expression', 'Unknown')
                results.append({"expression": calc_expression, "status": "failed", "error": str(e)})

        return {"status": "completed", "total_calculations": total_count, "results": results}
        
    except Exception as e:
        print(f"Automation failed: {e}")
        
        # Clean up
        try:
            close_calculator()
        except:
            pass
        
        # Since the flow caught an error, we re-raise to mark the flow run as failed
        raise
        
    finally:
        # The finally block ensures close_calculator runs even if the flow fails
        close_calculator()
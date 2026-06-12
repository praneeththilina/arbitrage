import asyncio
import sys
import os

# Add root folder to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import test files
import test_strategy_lifecycle
import test_swing_trading
import test_calibration

def run_tests():
    print("=" * 60)
    print("Running Custom Test Suite...")
    print("=" * 60)
    
    test_modules = [test_strategy_lifecycle, test_swing_trading, test_calibration]
    passed = 0
    failed = 0
    
    for mod in test_modules:
        mod_name = mod.__name__
        print(f"\nRunning tests in module: {mod_name}")
        
        for name in dir(mod):
            if name.startswith("test_"):
                func = getattr(mod, name)
                print(f" - {name} ... ", end="")
                try:
                    if asyncio.iscoroutinefunction(func):
                        # Use a clean event loop to run async test functions
                        asyncio.run(func())
                    else:
                        func()
                    print("PASS")
                    passed += 1
                except Exception as e:
                    print(f"FAIL: {e}")
                    import traceback
                    traceback.print_exc()
                    failed += 1
                    
    print("\n" + "=" * 60)
    print(f"Test Execution Completed. Passed: {passed}, Failed: {failed}")
    print("=" * 60)
    if failed > 0:
        sys.exit(1)

if __name__ == "__main__":
    if sys.platform == 'win32':
        # Don't call set_event_loop_policy here unless we are running asyncio.run
        pass
    run_tests()

import sys
sys.path.append("/src")  

from transform import Transform

def main():
    transformer = Transform()
    transformer.run()
    transformer.stop()

if __name__ == "__main__":
    main()

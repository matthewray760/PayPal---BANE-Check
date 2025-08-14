import tkinter as tk
from tkinter import messagebox, simpledialog


def show_popup(message):
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    messagebox.showinfo("Info", message)  # Show the message box
    root.destroy()  # Destroy the root window after closing the message box


def get_input(prompt):
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    user_input = simpledialog.askstring("Input", prompt)  # Get user input
    root.destroy()  # Destroy the root window after getting input
    return user_input

def calculate_growth(initial, rate, years):
    """Calculates compound growth."""
    return initial * (1 + rate) ** years

print(f"Growth after 5 years: {calculate_growth(100, 0.05, 5)}")

# 2. Add, Commit, and Push
repo_name = "per_python-test" # Use your repo name
%cd {repo_name}
!git add .
!git commit -m "Initial commit with sample script"
!git push origin main
%cd ..

print("🚀 Code is now on GitHub! Jules can now see your project.")
